#![deny(clippy::dbg_macro)]

use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use everscale_rpc_client::{ClientOptions, RpcClient};
use futures::{channel::oneshot, SinkExt, Stream, StreamExt};
use nekoton::transport::models::ExistingContract;
use rdkafka::{
    config::FromClientConfig,
    consumer::{CommitMode, Consumer, ConsumerContext, StreamConsumer},
    ClientConfig, Message, Offset, TopicPartitionList,
};
use ton_block::{Deserializable, MsgAddressInt};
use ton_block_compressor::ZstdWrapper;
use ton_types::UInt256;
use url::Url;

macro_rules! try_res {
    ($some:expr, $msg:literal) => {
        match $some {
            Ok(a) => a,
            Err(e) => {
                ::log::error!("{}:{:?}", $msg, e);
                continue;
            }
        }
    };
}

macro_rules! try_opt {
    ($some:expr, $msg:literal) => {
        match $some {
            Some(a) => a,
            None => {
                ::log::error!("{}", $msg);
                continue;
            }
        }
    };
}

pub struct TransactionConsumer {
    states_client: Option<RpcClient>,
    pub topic: String,
    config: ClientConfig,
    skip_0_partition: bool,
}

pub struct ConsumerOptions<'opts> {
    pub kafka_options: HashMap<&'opts str, &'opts str>,
    /// read from masterchain or not
    pub skip_0_partition: bool,
}

impl TransactionConsumer {
    /// [states_rpc_endpoint] - list of endpoints of states rpcs with /rpc suffix
    pub async fn new<I>(
        group_id: &str,
        topic: &str,
        states_rpc_endpoints: I,
        rpc_options: Option<ClientOptions>,
        options: ConsumerOptions<'_>,
    ) -> Result<Arc<Self>>
    where
        I: IntoIterator<Item = Url> + Send + Clone,
    {
        let client = RpcClient::new(states_rpc_endpoints, rpc_options.unwrap_or_default()).await?;
        Self::with_rpc_client(group_id, topic, client, options).await
    }

    pub async fn with_rpc_client(
        group_id: &str,
        topic: &str,
        states_client: RpcClient,
        options: ConsumerOptions<'_>,
    ) -> Result<Arc<Self>> {
        Ok(Arc::new(
            Self::new_inner(group_id, topic, Some(states_client), options).await,
        ))
    }

    pub async fn without_rpc_client(
        group_id: &str,
        topic: &str,
        options: ConsumerOptions<'_>,
    ) -> Result<Arc<Self>> {
        Ok(Arc::new(
            Self::new_inner(group_id, topic, None, options).await,
        ))
    }

    async fn new_inner(
        group_id: &str,
        topic: &str,
        states_client: Option<RpcClient>,
        options: ConsumerOptions<'_>,
    ) -> Self {
        let mut config = ClientConfig::default();
        config
            .set("group.id", group_id)
            .set("enable.auto.commit", "true")
            .set("auto.commit.interval.ms", "5000")
            .set("enable.auto.offset.store", "false")
            .set("auto.offset.reset", "earliest");

        for (k, v) in options.kafka_options {
            config.set(k, v);
        }

        Self {
            states_client,
            topic: topic.to_string(),
            config,
            skip_0_partition: options.skip_0_partition,
        }
    }

    fn subscribe(&self, stream_from: &StreamFrom) -> Result<Arc<StreamConsumer>> {
        let consumer = StreamConsumer::from_config(&self.config)?;
        let mut assignment = TopicPartitionList::new();

        let num_partitions = get_topic_partition_count(&consumer, &self.topic)?;
        let start = if self.skip_0_partition { 1 } else { 0 };
        for x in start..num_partitions {
            assignment.add_partition_offset(
                &self.topic,
                x as i32,
                stream_from
                    .get_offset(x as i32)
                    .with_context(|| format!("No offset for {x} partition"))?,
            )?;
        }

        log::info!("Assigning: {:?}", assignment);
        consumer.assign(&assignment)?;
        Ok(Arc::new(consumer))
    }

    pub async fn stream_transactions(
        &self,
        from: StreamFrom,
    ) -> Result<impl Stream<Item = ConsumedTransaction>> {
        let consumer = self.subscribe(&from)?;

        let (mut tx, rx) = futures::channel::mpsc::channel(1);

        log::info!("Starting streaming");
        tokio::spawn(async move {
            let mut decompressor = ZstdWrapper::new();
            let stream = consumer.stream();
            tokio::pin!(stream);
            while let Some(message) = stream.next().await {
                let message = try_res!(message, "Failed to get message");
                let payload = try_opt!(message.payload(), "no payload");
                let payload_decompressed = try_res!(
                    decompressor.decompress(payload),
                    "Failed decompressing block data"
                );

                tokio::task::yield_now().await;

                let transaction = try_res!(
                    ton_block::Transaction::construct_from_bytes(payload_decompressed),
                    "Failed constructing block"
                );
                let key = try_opt!(message.key(), "No key");
                let key = UInt256::from_slice(key);

                let (block, rx) = ConsumedTransaction::new(
                    key,
                    transaction,
                    message.offset(),
                    message.partition(),
                );
                if let Err(e) = tx.send(block).await {
                    log::error!("Failed sending via channel: {:?}", e); //todo panic?
                    return;
                }

                if rx.await.is_err() {
                    continue;
                }

                try_res!(
                    consumer.store_offset_from_message(&message),
                    "Failed committing"
                );
                log::debug!("Stored offsets");
            }
        });

        Ok(rx)
    }

    pub async fn stream_until_highest_offsets(
        &self,
        from: StreamFrom,
    ) -> Result<(impl Stream<Item = ConsumedTransaction>, Offsets)> {
        log::info!("Starting stream_until_highest_offsets {:?}", from);

        let (tx, rx) = futures::channel::mpsc::channel(1);
        let consumer: StreamConsumer = StreamConsumer::from_config(&self.config)?;
        let watermarks = get_partition_watermarks(&consumer, &self.topic, self.skip_0_partition)?;

        let offsets = Offsets(HashMap::from_iter(watermarks.iter().map(|watermark| {
            (
                watermark.partition,
                watermark.high.checked_sub(1).unwrap_or(0),
            )
        })));

        let stored: HashMap<i32, Offset> = if matches!(from, StreamFrom::Stored) {
            let mut tpl = TopicPartitionList::new();
            for watermark in &watermarks {
                if let Err(e) =
                    tpl.add_partition_offset(&self.topic, watermark.partition, Offset::Stored)
                {
                    log::warn!(
                        "Failed to get stored offset for {}: {:?}",
                        watermark.partition,
                        e
                    );
                    continue;
                }
            }
            log::debug!("Partitions added to tpl");
            let stored = consumer.committed_offsets(tpl, None)?;
            log::debug!("Stored commited offsets");
            stored
                .elements_for_topic(&self.topic)
                .iter()
                .map(|entry| (entry.partition(), entry.offset()))
                .collect()
        } else {
            HashMap::new()
        };
        log::debug!("Stored elements for topic");

        drop(consumer);
        let this = self;
        for watermark in watermarks {
            let part = watermark.partition;
            let committed_offset = if matches!(from, StreamFrom::Stored) {
                stored.get(&part).copied()
            } else {
                None
            };
            let Some(plan) = build_partition_stream_plan(
                &from,
                part,
                watermark.low,
                watermark.high,
                committed_offset,
            ) else {
                continue;
            };

            log::warn!(
                "Starting stream for partition {}. Reading offsets [{}..{})",
                part,
                plan.start_offset,
                plan.end_exclusive
            );
            log::debug!("Creating consumer for partition {}", part);

            let consumer: StreamConsumer = StreamConsumer::from_config(&this.config)?;
            log::debug!("Consumer created");
            let mut tx = tx.clone();
            log::debug!("Creating tpl");
            let mut tpl = TopicPartitionList::new();
            tpl.add_partition_offset(&this.topic, part, Offset::Offset(plan.start_offset))?;
            log::debug!("Added partition to tpl");
            consumer.assign(&tpl)?;
            log::debug!("Assigned tpl");

            tokio::spawn(async move {
                let stream = consumer.stream();
                tokio::pin!(stream);

                let mut decompressor = ZstdWrapper::new();

                while let Some(message) = stream.next().await {
                    let message = try_res!(message, "Failed to get message");

                    let payload = try_opt!(message.payload(), "no payload");
                    let payload_decompressed = try_res!(
                        decompressor.decompress(payload),
                        "Failed decompressing block data"
                    );

                    tokio::task::yield_now().await;

                    let transaction = try_res!(
                        ton_block::Transaction::construct_from_bytes(payload_decompressed),
                        "Failed constructing block"
                    );
                    let key = try_opt!(message.key(), "No key");
                    let key = UInt256::from_slice(key);

                    let (block, rx) = ConsumedTransaction::new(
                        key,
                        transaction,
                        message.offset(),
                        message.partition(),
                    );
                    if let Err(e) = tx.send(block).await {
                        log::error!("Failed sending via channel: {:?}", e); //todo panic?
                        return;
                    }

                    let offset = message.offset();
                    if offset.saturating_add(1) >= plan.end_exclusive {
                        log::info!(
                            "Reached snapshot end: {} + 1 >= {}. Partition: {}",
                            offset,
                            plan.end_exclusive,
                            part
                        );
                        if let Err(e) = consumer.commit_message(&message, CommitMode::Sync) {
                            log::error!("Failed committing final message: {:?}", e);
                        }
                        break;
                    }

                    if rx.await.is_err() {
                        continue;
                    }

                    try_res!(
                        consumer.store_offset_from_message(&message),
                        "Failed committing"
                    );
                    log::debug!("Stored offsets");
                }
            });
            log::debug!("Spawned task for partition {}", part);
        }
        log::debug!("Returning rx");
        Ok((rx, offsets))
    }

    pub async fn stream_until_highest_offsets_with_manual_commit(
        &self,
        from: StreamFrom,
    ) -> Result<(impl Stream<Item = ConsumedTransactionWithMessage>, Offsets)> {
        log::debug!("Starting stream_until_highest_offsets");
        let consumer: StreamConsumer = StreamConsumer::from_config(&self.config)?;
        log::debug!("Consumer created");
        let (tx, rx) = futures::channel::mpsc::channel(1);

        let watermarks = get_partition_watermarks(&consumer, &self.topic, self.skip_0_partition)?;
        let stored: HashMap<i32, Offset> = if matches!(from, StreamFrom::Stored) {
            let mut tpl = TopicPartitionList::new();
            for watermark in &watermarks {
                if let Err(e) =
                    tpl.add_partition_offset(&self.topic, watermark.partition, Offset::Stored)
                {
                    log::warn!(
                        "Failed to get stored offset for {}: {:?}",
                        watermark.partition,
                        e
                    );
                    continue;
                }
            }
            log::debug!("Partitions added to tpl");
            let stored = consumer.committed_offsets(tpl, None)?;
            log::debug!("Stored commited offsets");
            stored
                .elements_for_topic(&self.topic)
                .iter()
                .map(|entry| (entry.partition(), entry.offset()))
                .collect()
        } else {
            HashMap::new()
        };
        log::debug!("Stored elements for topic");

        let offsets = Offsets(HashMap::from_iter(watermarks.iter().map(|watermark| {
            (
                watermark.partition,
                watermark.high.checked_sub(1).unwrap_or(0),
            )
        })));
        log::debug!("Offsets created");

        let this = self;
        drop(consumer);
        for watermark in watermarks {
            let partition = watermark.partition;
            let plan = if matches!(from, StreamFrom::End) {
                // Preserve legacy End behavior in manual-commit mode:
                // consume first new message (if any) per partition, then stop that partition.
                Some(build_tail_like_plan_for_end(watermark.high))
            } else {
                let committed_offset = if matches!(from, StreamFrom::Stored) {
                    stored.get(&partition).copied()
                } else {
                    None
                };
                build_partition_stream_plan(
                    &from,
                    partition,
                    watermark.low,
                    watermark.high,
                    committed_offset,
                )
            };
            let Some(plan) = plan else {
                continue;
            };

            log::warn!(
                "Starting stream for partition {partition}. Reading offsets [{}..{})",
                plan.start_offset,
                plan.end_exclusive
            );
            log::debug!("Creating consumer for partition {partition}");

            let consumer: StreamConsumer = StreamConsumer::from_config(&this.config)?;
            log::debug!("Consumer created");
            let mut tx = tx.clone();
            log::debug!("Creating tpl");
            let mut tpl = TopicPartitionList::new();
            tpl.add_partition_offset(&this.topic, partition, Offset::Offset(plan.start_offset))?;
            log::debug!("Added partition to tpl");
            consumer.assign(&tpl)?;
            log::debug!("Assigned tpl");

            tokio::spawn({
                let consumer = Arc::new(consumer);
                async move {
                    let stream = consumer.stream();
                    tokio::pin!(stream);

                    let mut decompressor = ZstdWrapper::new();

                    while let Some(message) = stream.next().await {
                        let message = try_res!(message, "Failed to get message");

                        let payload = try_opt!(message.payload(), "No payload");
                        let payload_decompressed = try_res!(
                            decompressor.decompress(payload),
                            "Failed decompressing block data"
                        );

                        tokio::task::yield_now().await;

                        let transaction = try_res!(
                            ton_block::Transaction::construct_from_bytes(payload_decompressed),
                            "Failed constructing block"
                        );
                        let key = try_opt!(message.key(), "No key");
                        let key = UInt256::from_slice(key);

                        let consumed_transaction = ConsumedTransactionWithMessage::new(
                            key,
                            transaction,
                            message.offset(),
                            message.partition(),
                            consumer.clone(),
                        );
                        if let Err(e) = tx.send(consumed_transaction).await {
                            log::error!("Failed sending via channel: {:?}", e); //todo panic?
                            return;
                        }

                        let offset = message.offset();
                        if offset.saturating_add(1) >= plan.end_exclusive {
                            log::info!(
                                "Reached snapshot end: {offset} + 1 >= {}. Partition: {partition}",
                                plan.end_exclusive,
                            );
                            if let Err(e) = consumer.commit_message(&message, CommitMode::Sync) {
                                log::error!("Failed committing final message: {:?}", e);
                            }
                            break;
                        }

                        log::debug!("Stored offsets");
                    }
                }
            });
            log::debug!("Spawned task for partition {partition}",);
        }
        log::debug!("Returning rx");
        Ok((rx, offsets))
    }

    pub async fn stream_with_manual_commit(
        &self,
        offsets: Offsets,
    ) -> Result<impl Stream<Item = ConsumedTransactionWithMessage>> {
        log::debug!("Starting stream_with_manual_commit");
        let (tx, rx) = futures::channel::mpsc::channel(1);

        let this = self;

        for (partition, offset_for_partition) in offsets.0 {
            log::debug!("Creating consumer for partition {partition}");

            let consumer: StreamConsumer = StreamConsumer::from_config(&this.config)?;
            let mut tx = tx.clone();
            let mut tpl = TopicPartitionList::new();
            tpl.add_partition_offset(&this.topic, partition, Offset::Offset(offset_for_partition))?;
            consumer.assign(&tpl)?;

            tokio::spawn({
                let consumer = Arc::new(consumer);
                async move {
                    let stream = consumer.stream();
                    tokio::pin!(stream);

                    let mut decompressor = ZstdWrapper::new();

                    while let Some(message) = stream.next().await {
                        let message = try_res!(message, "Failed to get message");

                        let payload = try_opt!(message.payload(), "no payload");
                        let payload_decompressed = try_res!(
                            decompressor.decompress(payload),
                            "Failed decompressing block data"
                        );

                        tokio::task::yield_now().await;

                        let transaction = try_res!(
                            ton_block::Transaction::construct_from_bytes(payload_decompressed),
                            "Failed constructing block"
                        );
                        let key = try_opt!(message.key(), "No key");
                        let key = UInt256::from_slice(key);

                        let consumed_transaction = ConsumedTransactionWithMessage::new(
                            key,
                            transaction,
                            message.offset(),
                            message.partition(),
                            consumer.clone(),
                        );
                        if let Err(e) = tx.send(consumed_transaction).await {
                            log::error!("Failed sending via channel: {:?}", e); //todo panic?
                            return;
                        }
                    }
                }
            });
            log::debug!("Spawned task for partition {partition}");
        }
        log::debug!("Returning rx");
        Ok(rx)
    }

    pub async fn get_contract_state(
        &self,
        contract_address: &MsgAddressInt,
    ) -> Result<Option<ExistingContract>> {
        if let Some(states_client) = &self.states_client {
            states_client
                .get_contract_state(contract_address, None)
                .await
        } else {
            anyhow::bail!("Missing states client")
        }
    }

    pub async fn run_local(
        &self,
        contract_address: &MsgAddressInt,
        function: &ton_abi::Function,
        input: &[ton_abi::Token],
    ) -> Result<Option<nekoton_abi::ExecutionOutput>> {
        if let Some(states_client) = &self.states_client {
            states_client
                .run_local(contract_address, function, input)
                .await
        } else {
            anyhow::bail!("Missing states client")
        }
    }

    pub fn get_client(&self) -> &Option<RpcClient> {
        &self.states_client
    }
}

#[derive(Debug, Clone)]
pub enum StreamFrom {
    Beginning,
    Stored,
    End,
    Offsets(Offsets),
}

impl StreamFrom {
    pub fn get_offset(&self, part: i32) -> Option<Offset> {
        match &self {
            StreamFrom::Beginning => Some(Offset::Beginning),
            StreamFrom::Stored => Some(Offset::Stored),
            StreamFrom::End => Some(Offset::End),
            StreamFrom::Offsets(offsets) => offsets.0.get(&part).map(|x| Offset::Offset(*x)),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Offsets(pub HashMap<i32, i64>);

pub struct ConsumedTransaction {
    pub id: UInt256,
    pub transaction: ton_block::Transaction,

    pub offset: i64,
    pub partition: i32,
    commit_channel: Option<oneshot::Sender<()>>,
}

impl ConsumedTransaction {
    fn new(
        id: UInt256,
        transaction: ton_block::Transaction,
        offset: i64,
        partition: i32,
    ) -> (Self, oneshot::Receiver<()>) {
        let (tx, rx) = oneshot::channel();
        (
            Self {
                id,
                transaction,
                offset,
                partition,
                commit_channel: Some(tx),
            },
            rx,
        )
    }

    pub fn commit(mut self) -> Result<()> {
        let committer = self.commit_channel.take().context("Already committed")?;
        committer
            .send(())
            .map_err(|_| anyhow::anyhow!("Failed committing"))?;
        Ok(())
    }

    pub fn into_inner(self) -> (UInt256, ton_block::Transaction) {
        (self.id, self.transaction)
    }

    pub fn into_inner_with_commit_channel(
        self,
    ) -> (UInt256, ton_block::Transaction, oneshot::Sender<()>) {
        (self.id, self.transaction, self.commit_channel.unwrap())
    }
}

pub struct ConsumedMessage {
    pub offset: i64,
    pub partition: i32,
    consumer: Arc<StreamConsumer>,
}

impl ConsumedMessage {
    pub fn store_offset(&self, topic: &str) -> Result<()> {
        self.consumer
            .store_offset(topic, self.partition, self.offset)
            .map_err(|e| anyhow::anyhow!("Store offset Failed: {e}"))?;
        Ok(())
    }
}

pub struct ConsumedTransactionWithMessage {
    pub id: UInt256,
    pub transaction: ton_block::Transaction,
    pub message: ConsumedMessage,
}

impl ConsumedTransactionWithMessage {
    fn new(
        id: UInt256,
        transaction: ton_block::Transaction,
        offset: i64,
        partition: i32,
        consumer: Arc<StreamConsumer>,
    ) -> Self {
        Self {
            id,
            transaction,
            message: ConsumedMessage {
                offset,
                partition,
                consumer,
            },
        }
    }

    pub fn into_inner(self) -> (UInt256, ton_block::Transaction, ConsumedMessage) {
        (self.id, self.transaction, self.message)
    }
}

#[derive(Debug, Clone, Copy)]
struct PartitionWatermark {
    partition: i32,
    low: i64,
    high: i64,
}

#[derive(Debug, Clone, Copy)]
struct PartitionStreamPlan {
    start_offset: i64,
    end_exclusive: i64,
}

fn build_partition_stream_plan(
    from: &StreamFrom,
    partition: i32,
    low: i64,
    high: i64,
    committed_offset: Option<Offset>,
) -> Option<PartitionStreamPlan> {
    if low >= high {
        log::warn!(
            "Skipping partition {}. Empty snapshot range: low={}, high={}",
            partition,
            low,
            high
        );
        return None;
    }

    let mut start_offset = match from {
        StreamFrom::Beginning => low,
        StreamFrom::End => high,
        StreamFrom::Offsets(offsets) => {
            let Some(offset) = offsets.0.get(&partition).copied() else {
                log::warn!("No offset for partition {}", partition);
                return None;
            };
            offset
        }
        StreamFrom::Stored => match committed_offset {
            Some(Offset::Offset(offset)) => offset,
            Some(Offset::Invalid) => {
                log::warn!(
                    "Invalid stored offset for partition {}. Falling back to low watermark {}",
                    partition,
                    low
                );
                low
            }
            Some(Offset::Beginning) => {
                log::warn!(
                    "Stored offset marker Beginning for partition {}. Using low watermark {}",
                    partition,
                    low
                );
                low
            }
            Some(Offset::End) => {
                log::warn!(
                    "Stored offset marker End for partition {}. Using high watermark {}",
                    partition,
                    high
                );
                high
            }
            Some(Offset::OffsetTail(delta)) => {
                let start = high.saturating_sub(delta);
                log::warn!(
                    "Stored offset marker OffsetTail({}) for partition {}. Using start {}",
                    delta,
                    partition,
                    start
                );
                start
            }
            Some(Offset::Stored) => {
                log::warn!(
                    "Stored offset marker Stored for partition {}. Falling back to low watermark {}",
                    partition,
                    low
                );
                low
            }
            None => {
                log::warn!(
                    "No stored offset for partition {}. Falling back to low watermark {}",
                    partition,
                    low
                );
                low
            }
        },
    };

    if start_offset < low {
        log::warn!(
            "Clamping start offset for partition {} from {} to low watermark {}",
            partition,
            start_offset,
            low
        );
        start_offset = low;
    }

    if start_offset >= high {
        log::warn!(
            "Skipping partition {}. Already synced for snapshot: start={}, high={}",
            partition,
            start_offset,
            high
        );
        return None;
    }

    Some(PartitionStreamPlan {
        start_offset,
        end_exclusive: high,
    })
}

fn build_tail_like_plan_for_end(high: i64) -> PartitionStreamPlan {
    PartitionStreamPlan {
        start_offset: high,
        end_exclusive: high.saturating_add(1),
    }
}

fn get_topic_partition_count<X: ConsumerContext, C: Consumer<X>>(
    consumer: &C,
    topic_name: &str,
) -> Result<usize> {
    let metadata = consumer
        .fetch_metadata(Some(topic_name), Duration::from_secs(30))
        .context("Failed to fetch metadata")?;

    if metadata.topics().is_empty() {
        anyhow::bail!("Topics is empty")
    }

    let partitions = metadata
        .topics()
        .iter()
        .find(|x| x.name() == topic_name)
        .map(|x| x.partitions().iter().count())
        .context("No such topic")?;
    Ok(partitions)
}

fn get_partition_watermarks<X: ConsumerContext, C: Consumer<X>>(
    consumer: &C,
    topic_name: &str,
    skip_0_partition: bool,
) -> Result<Vec<PartitionWatermark>> {
    log::debug!("Getting partition watermarks for {}", topic_name);
    let topic_partition_count = get_topic_partition_count(consumer, topic_name)?;
    log::debug!("Get topic partition count: {}", topic_partition_count);
    let mut parts_info = Vec::with_capacity(topic_partition_count);
    let start = if skip_0_partition { 1 } else { 0 };

    for part in start..topic_partition_count {
        log::debug!("Getting watermarks for partition {}", part);
        let (low, high) = consumer
            .fetch_watermarks(topic_name, part as i32, Duration::from_secs(30))
            .with_context(|| format!("Failed to fetch offset {}", part))?;
        parts_info.push(PartitionWatermark {
            partition: part as i32,
            low,
            high,
        });
    }
    log::debug!("Got partition watermarks");
    Ok(parts_info)
}

#[cfg(test)]
mod test {
    use std::{collections::HashMap, str::FromStr};

    use rdkafka::Offset;
    use ton_block::MsgAddressInt;

    use crate::{
        build_partition_stream_plan, build_tail_like_plan_for_end, ConsumerOptions, Offsets,
        StreamFrom, TransactionConsumer,
    };

    #[test]
    fn planner_skips_empty_snapshot_partition() {
        let plan =
            build_partition_stream_plan(&StreamFrom::Stored, 1, 10, 10, Some(Offset::Offset(10)));
        assert!(plan.is_none());
    }

    #[test]
    fn planner_skips_when_stored_is_already_at_high() {
        let plan =
            build_partition_stream_plan(&StreamFrom::Stored, 1, 0, 100, Some(Offset::Offset(100)));
        assert!(plan.is_none());
    }

    #[test]
    fn planner_keeps_last_message_when_stored_is_high_minus_one() {
        let plan =
            build_partition_stream_plan(&StreamFrom::Stored, 1, 0, 100, Some(Offset::Offset(99)))
                .expect("plan should be created");
        assert_eq!(plan.start_offset, 99);
        assert_eq!(plan.end_exclusive, 100);
    }

    #[test]
    fn planner_uses_low_watermark_for_invalid_stored_offset() {
        let plan =
            build_partition_stream_plan(&StreamFrom::Stored, 1, 15, 30, Some(Offset::Invalid))
                .expect("plan should be created");
        assert_eq!(plan.start_offset, 15);
        assert_eq!(plan.end_exclusive, 30);
    }

    #[test]
    fn planner_does_not_rewind_when_stored_offset_is_end_marker() {
        let plan = build_partition_stream_plan(&StreamFrom::Stored, 1, 15, 30, Some(Offset::End));
        assert!(plan.is_none());
    }

    #[test]
    fn planner_clamps_explicit_offset_to_low_watermark() {
        let plan = build_partition_stream_plan(
            &StreamFrom::Offsets(Offsets(HashMap::from([(7, 5)]))),
            7,
            10,
            20,
            None,
        )
        .expect("plan should be created");
        assert_eq!(plan.start_offset, 10);
        assert_eq!(plan.end_exclusive, 20);
    }

    #[test]
    fn end_manual_plan_reads_first_new_message_only() {
        let plan = build_tail_like_plan_for_end(42);
        assert_eq!(plan.start_offset, 42);
        assert_eq!(plan.end_exclusive, 43);
    }

    #[tokio::test]
    async fn test_get() {
        let pr = TransactionConsumer::new(
            "some_group",
            "some_topic",
            vec!["https://jrpc.everwallet.net/rpc".parse().unwrap()],
            None,
            ConsumerOptions {
                kafka_options: Default::default(),
                skip_0_partition: false,
            },
        )
        .await
        .unwrap();

        pr.get_contract_state(
            &MsgAddressInt::from_str(
                "0:8e2586602513e99a55fa2be08561469c7ce51a7d5a25977558e77ef2bc9387b4",
            )
            .unwrap(),
        )
        .await
        .unwrap()
        .unwrap();

        pr.get_contract_state(
            &MsgAddressInt::from_str(
                "-1:efd5a14409a8a129686114fc092525fddd508f1ea56d1b649a3a695d3a5b188c",
            )
            .unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(pr
            .get_contract_state(
                &MsgAddressInt::from_str(
                    "-1:aaa5a14409a8a129686114fc092525fddd508f1ea56d1b649a3a695d3a5b188c",
                )
                .unwrap(),
            )
            .await
            .unwrap()
            .is_none());
    }
}
