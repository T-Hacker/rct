use rust_decimal::Decimal;
use serde::Deserialize;

pub enum TransactionType {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

#[derive(Debug, Deserialize)]
pub struct Transaction {
    #[serde(rename = "type")]
    ttype: String,

    client: u16,
    tx: u32,
    amount: Option<Decimal>,
}

impl Transaction {
    #[allow(dead_code)]
    pub fn new(
        transaction_type: TransactionType,
        client: u16,
        tx: u32,
        amount: Option<Decimal>,
    ) -> Self {
        let ttype = match transaction_type {
            TransactionType::Deposit => "deposit",
            TransactionType::Withdrawal => "withdrawal",
            TransactionType::Dispute => "dispute",
            TransactionType::Resolve => "resolve",
            TransactionType::Chargeback => "chargeback",
        }
        .into();

        Self {
            ttype,
            client,
            tx,
            amount,
        }
    }

    pub fn get_type(&self) -> Option<TransactionType> {
        let type_str = self.ttype.to_ascii_lowercase();
        match type_str.as_str() {
            "deposit" => Some(TransactionType::Deposit),
            "withdrawal" => Some(TransactionType::Withdrawal),
            "dispute" => Some(TransactionType::Dispute),
            "resolve" => Some(TransactionType::Resolve),
            "chargeback" => Some(TransactionType::Chargeback),

            _ => None,
        }
    }

    pub fn get_client_id(&self) -> u16 {
        self.client
    }

    pub fn get_tx_id(&self) -> u32 {
        self.tx
    }

    pub fn get_amount(&self) -> &Option<Decimal> {
        &self.amount
    }
}
