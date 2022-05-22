use crate::{
    client::Client,
    transaction::{Transaction, TransactionType},
};
use anyhow::{Error, Result};
use std::collections::HashMap;
use tokio::{sync::mpsc, task::JoinHandle};

/// Process transactions in parallel by distributing them to workers by their client id.
pub struct TransactionProcessor {
    join_handle: JoinHandle<Result<HashMap<u16, Client>, Error>>,
}

impl TransactionProcessor {
    pub fn new(transaction_rx: mpsc::UnboundedReceiver<Transaction>) -> Self {
        // Create the load balancer.
        let join_handle = tokio::spawn(Self::load_balancer(transaction_rx));

        Self { join_handle }
    }

    pub async fn get_results(self) -> Result<HashMap<u16, Client>, Error> {
        self.join_handle.await?
    }

    /// This load balancer uses the client' id to find which worker should process the transaction.
    /// It is a very basic load balancer but has a convenient property: A single worker is responsible for
    /// managing the client state. The clients don't migrate between workers, that way the worker doesn't
    /// need to use any locking mechanism to access the client data, since it's local to the worker in question.
    async fn load_balancer(
        mut rx: mpsc::UnboundedReceiver<Transaction>,
    ) -> Result<HashMap<u16, Client>> {
        let worker_join_handlers = {
            let workers = (0..num_cpus::get())
                .map(|_| mpsc::unbounded_channel::<Transaction>())
                .map(|(tx, rx)| {
                    let join_handle = tokio::spawn(Self::worker(rx));

                    (tx, join_handle)
                })
                .collect::<Vec<_>>();

            let workers_len = workers.len() as u16;

            while let Some(transaction) = rx.recv().await {
                // Simple load balance by client id.
                let worker_index = transaction.get_client_id() % workers_len;

                let (tx, _) = &workers[worker_index as usize];
                tx.send(transaction)?;
            }

            workers.into_iter().map(|(_, join_handle)| join_handle)
        };

        let mut results = HashMap::new();
        for join_handle in worker_join_handlers {
            let result = join_handle.await?;

            results.extend(result);
        }

        Ok(results)
    }

    async fn worker(mut rx: mpsc::UnboundedReceiver<Transaction>) -> HashMap<u16, Client> {
        let mut clients = HashMap::new();
        let mut transactions: HashMap<u32, Transaction> = Default::default();

        while let Some(transaction) = rx.recv().await {
            let client = clients
                .entry(transaction.get_client_id())
                .or_insert_with(|| Client::new(transaction.get_client_id()));

            if !client.is_locked() {
                if let Some(transaction_type) = transaction.get_type() {
                    match transaction_type {
                        TransactionType::Deposit => {
                            if let Some(amount) = transaction.get_amount() {
                                if client.add_available(*amount).is_ok() {
                                    transactions.insert(transaction.get_tx_id(), transaction);
                                }
                            }
                        }

                        TransactionType::Withdrawal => {
                            if let Some(amount) = transaction.get_amount() {
                                if client.subtract_available(*amount).is_ok() {
                                    transactions.insert(transaction.get_tx_id(), transaction);
                                }
                            }
                        }

                        TransactionType::Dispute => {
                            if let Some(ref_transaction) =
                                transactions.get(&transaction.get_tx_id())
                            {
                                if ref_transaction.get_client_id() == client.get_id() {
                                    if let Some(amount) = ref_transaction.get_amount() {
                                        client
                                            .transfer_available_to_held(*amount)
                                            .unwrap_or_default();
                                    }
                                }
                            }
                        }

                        TransactionType::Resolve => {
                            if let Some(ref_transaction) =
                                transactions.get(&transaction.get_tx_id())
                            {
                                if ref_transaction.get_client_id() == client.get_id() {
                                    if let Some(amount) = ref_transaction.get_amount() {
                                        client
                                            .transfer_held_to_available(*amount)
                                            .unwrap_or_default();
                                    }
                                }
                            }
                        }

                        TransactionType::Chargeback => {
                            if let Some(ref_transaction) =
                                transactions.get(&transaction.get_tx_id())
                            {
                                if ref_transaction.get_client_id() == client.get_id() {
                                    if let Some(amount) = ref_transaction.get_amount() {
                                        client.subtract_held(*amount).unwrap_or_default();
                                        client.lock_account();
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        clients
    }
}
