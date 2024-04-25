use serde::{Deserialize, Serialize};
#[derive(Debug, Serialize, Deserialize)]
pub struct GqlTransaction {
    pub json_version: i64,
    pub id: String,
    pub block_id: String,
    pub boc: String,
    pub chain_order: String,
}

#[cfg(test)]
mod test {
    use super::*;
    use ton_block::Deserializable;
    #[test]
    fn test_de() {
        let tx_data = r#"{"json_version":8,"id":"8f16b72867f85e8fca6c28820893b5d047bae65a10278ceb61be341cd3bb50f7","block_id":"0bf4ffdb95c1d442fc3b0c5891a29ff198ac83de8d7b2193c306fa1136711fd9","boc":"te6ccgECDAEAAroAA7VwM9J7n33/683s4RL8jGL04PzBkseWVW5EMzfyFm96s6AAAMck3a9AFMaJ4Y7fMvHrNU4xsazOXzlfukpz7zXkFj5p1YjQRFnAAADHJH5RMBZipQvwADRjVfjoBQQBAg8MQMYYrgLkQAMCAG3JgMNQSgjQAAAAAAACAAAAAAADkb0GsxejJr7mRCu5147jJQJxjVgWo4A/fRsT5AYskPJAUB9kAJ1CkOMTiAAAAAAAAAAAHcAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAgAIJy35gudAvFNsGuu87vkHMAZYdet7GTs/549mhvYaL7m5zRKw+RIl2Xyvk7vT2+hXpspmmIDQvo4RCc/x0vXcLqsgIB4AgGAQHfBwD7SAAGek9z77/9eb2cIl+RjF6cH5gyWPLKrciGZv5Cze9WdQAL7s0943of/a0tL1brivJglQXxVNpzOzeyarVCA0pVdZDuaygABgII2AAAGOSbtegEzFShfijiIGZAADPSe599/+vN7OES/Ixi9OD8wZLHllVuRDM38hZverOoAUWIAAZ6T3Pvv/15vZwiX5GMXpwfmDJY8sqtyIZm/kLN71Z0DAkB4aHxahV7Nrani3lB89kVjkiTyQ39ejev19zu0ybFDdLDoNthQdBr4VCk/+8F025rdgMwVOkLn1e7iJfRTzviegLlxS1JYLtB5MtvEYo/ev/2JwnA/ji2YYbnJHzpNeR62MAAAGPFUtpcmYqUPpM7mRsgCgFlgAX3Zp7xvQ/+1paXq3XFeTBKgviqbTmdm9k1WqEBpSq6wAAAAAAAAAAAAAAAB3NZQAA4CwBLUcRAzIAAZ6T3Pvv/15vZwiX5GMXpwfmDJY8sqtyIZm/kLN71Z1A=","status":3,"storage":{"storage_fees_collected_dec":"3","storage_fees_collected":"003","status_change":0},"compute":{"success":true,"msg_state_used":false,"account_activated":false,"gas_fees_dec":"1425500","gas_fees":"0515c05c","gas_used":5255,"gas_limit":0,"gas_credit":10000,"mode":0,"exit_code":0,"vm_steps":119,"vm_init_state_hash":"0000000000000000000000000000000000000000000000000000000000000000","vm_final_state_hash":"0000000000000000000000000000000000000000000000000000000000000000","compute_type":1},"action":{"success":true,"valid":true,"no_funds":false,"status_change":0,"total_fwd_fees_dec":"100000","total_fwd_fees":"04186a0","total_action_fees_dec":"33332","total_action_fees":"038234","result_code":0,"tot_actions":1,"spec_actions":0,"skipped_actions":0,"msgs_created":1,"action_list_hash":"c8de83598bd1935f732215dcebc771928138c6ac0b51c01fbe8d89f203164879","tot_msg_size_cells":1,"tot_msg_size_bits":1004},"credit_first":true,"aborted":false,"destroyed":false,"tr_type":0,"lt_dec":"13685072000001","lt":"ac724ddaf401","prev_trans_hash":"4c689e18edf32f1eb354e31b1acce5f395fba4a73ef35e4163e69d588d04459c","prev_trans_lt_dec":"13684972000001","prev_trans_lt":"ac7247e51301","now":1714049215,"outmsg_cnt":1,"orig_status":1,"end_status":1,"in_msg":"b59e7ef3e4b730972428c5de3776ee70290cef6385524c1f08ce17969882d0bd","ext_in_msg_fee_dec":"290100","ext_in_msg_fee":"0446d34","out_msgs":["b3b6dd9565d60ac397907c56dcf2b353151e4e91a2d23cf44ca06b2b2d99b55d"],"account_addr":"0:033d27b9f7dffebcdece112fc8c62f4e0fcc192c796556e443337f2166f7ab3a","workchain_id":0,"total_fees_dec":"1748935","total_fees":"051aafc7","balance_delta_dec":"-1001815603","balance_delta":"-f8c44981cc","old_hash":"df982e740bc536c1aebbceef90730065875eb7b193b3fe78f6686f61a2fb9b9c","new_hash":"d12b0f91225d97caf93bbd3dbe857a6ca669880d0be8e1109cff1d2f5dc2eab2","chain_order":"516d59000569978c0800"}"#;
        let tx: GqlTransaction = serde_json::from_str(tx_data).unwrap();
        ton_block::Transaction::construct_from_base64(&tx.boc).unwrap();
    }
}
