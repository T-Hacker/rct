mod client;
mod transaction;
mod transaction_processor;

use anyhow::{Context, Result};
use csv_async::Trim;
use futures::stream::StreamExt;
use tokio::sync::mpsc;
use transaction::Transaction;
use transaction_processor::TransactionProcessor;

#[tokio::main]
async fn main() -> Result<()> {
    // Handle application arguments.
    let mut args = std::env::args();
    let exe_name = args.next().context("Unable to get executable name.")?;
    let transactions_file_path = args
        .next()
        .context(format!("Usage: {exe_name} <transactions.csv>"))?;

    // Process transactions.
    let results = {
        // Create the channel and the transaction processor.
        let (client_tx, client_rx) = mpsc::unbounded_channel();
        let clients = TransactionProcessor::new(client_rx);

        // Open the CSV file with the transactions to be processed.
        let transaction_file = tokio::fs::File::open(transactions_file_path).await?;

        // Construct a CVS reader to parse the file.
        let mut reader = csv_async::AsyncReaderBuilder::new()
            .trim(Trim::All) // Make sure we trim everything to avoid parsing errors.
            .create_reader(transaction_file);

        // Submit all transactions to be processed in parallel.
        let mut records = reader.records();
        while let Some(record) = records.next().await {
            if let Ok(record) = record {
                let transaction = record.deserialize::<Transaction>(None);
                if let Ok(transaction) = transaction {
                    client_tx.send(transaction)?;
                }
            }
        }

        // We get the results future but we don't await for them here. We need to drop the 'client_tx' to
        // inform the transaction processor that we don't have any more data to process. Otherwise will be
        // in a deadlock state.
        clients.get_results()
    }
    .await?;

    // Output results.
    let mut writer = csv_async::AsyncWriter::from_writer(tokio::io::stdout());
    writer
        .write_record(&["client", "available", "held", "total", "locked"])
        .await?;

    for (_, client) in results {
        writer
            .write_record(&[
                client.get_id().to_string(),
                client.get_available().to_string(),
                client.get_held().to_string(),
                client.get_total().to_string(),
                client.is_locked().to_string(),
            ])
            .await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transaction::TransactionType;
    use rust_decimal::Decimal;

    /// Test if the system is capable of performing a valid deposit.
    #[tokio::test]
    async fn test_valid_deposit() {
        let tp = {
            let (tp_tx, tp_rx) = mpsc::unbounded_channel();
            let tp = TransactionProcessor::new(tp_rx);

            // Deposit 10 credits.
            tp_tx
                .send(Transaction::new(
                    TransactionType::Deposit,
                    1,
                    1,
                    Some(Decimal::new(10, 0)),
                ))
                .unwrap();

            tp
        };

        let clients = tp.get_results().await.unwrap();
        let client = clients.get(&1).unwrap();

        // Check if we have the 10 credits we deposited.
        assert_eq!(client.get_total(), Decimal::new(10, 0));
    }

    /// Test if the system is capable of performing a withdrawal.
    #[tokio::test]
    async fn test_valid_withdraw() {
        let tp = {
            let (tp_tx, tp_rx) = mpsc::unbounded_channel();
            let tp = TransactionProcessor::new(tp_rx);

            // We deposit 10 credits.
            tp_tx
                .send(Transaction::new(
                    TransactionType::Deposit,
                    1,
                    1,
                    Some(Decimal::new(10, 0)),
                ))
                .unwrap();

            // We withdraw 9 credits
            tp_tx
                .send(Transaction::new(
                    TransactionType::Withdrawal,
                    1,
                    2,
                    Some(Decimal::new(9, 0)),
                ))
                .unwrap();

            tp
        };

        let clients = tp.get_results().await.unwrap();
        let client = clients.get(&1).unwrap();

        assert_eq!(client.get_total(), Decimal::new(1, 0)); // We should have 1 credit left.
        assert_eq!(client.is_locked(), false); // The account should not be locked.
    }

    /// Test if the system is handles invalid deposits.
    #[tokio::test]
    async fn test_invalid_deposit() {
        let tp = {
            let (tp_tx, tp_rx) = mpsc::unbounded_channel();
            let tp = TransactionProcessor::new(tp_rx);

            // We try to deposit a negative 10 credits.
            tp_tx
                .send(Transaction::new(
                    TransactionType::Deposit,
                    1,
                    1,
                    Some(Decimal::new(-10, 0)),
                ))
                .unwrap();

            tp
        };

        let clients = tp.get_results().await.unwrap();
        let client = clients.get(&1).unwrap();

        assert_eq!(client.get_total(), Decimal::new(0, 0)); // We should still have zero credits.
        assert_eq!(client.is_locked(), false); // The account should not be locked.
    }

    /// Test how the system handles an invalid withdrawal.
    #[tokio::test]
    async fn test_invalid_withdraw() {
        let tp = {
            let (tp_tx, tp_rx) = mpsc::unbounded_channel();
            let tp = TransactionProcessor::new(tp_rx);

            // Deposit 10 credits.
            tp_tx
                .send(Transaction::new(
                    TransactionType::Deposit,
                    1,
                    1,
                    Some(Decimal::new(10, 0)),
                ))
                .unwrap();

            // Try the withdrawal 11 credits!
            tp_tx
                .send(Transaction::new(
                    TransactionType::Withdrawal,
                    1,
                    2,
                    Some(Decimal::new(11, 0)),
                ))
                .unwrap();

            tp
        };

        let clients = tp.get_results().await.unwrap();
        let client = clients.get(&1).unwrap();

        assert_eq!(client.get_total(), Decimal::new(10, 0)); // We should have the initial amount.
        assert_eq!(client.is_locked(), false); // The account should not be locked.
    }

    /// Test a scenario where a dispute was resolved.
    #[tokio::test]
    async fn test_resolved_dispute() {
        let tp = {
            let (tp_tx, tp_rx) = mpsc::unbounded_channel();
            let tp = TransactionProcessor::new(tp_rx);

            // Deposit 10 credits.
            tp_tx
                .send(Transaction::new(
                    TransactionType::Deposit,
                    1,
                    1,
                    Some(Decimal::new(10, 0)),
                ))
                .unwrap();

            // Deposit 5 more.
            tp_tx
                .send(Transaction::new(
                    TransactionType::Deposit,
                    1,
                    2,
                    Some(Decimal::new(5, 0)),
                ))
                .unwrap();

            // Dispute the last transaction.
            tp_tx
                .send(Transaction::new(TransactionType::Dispute, 1, 2, None))
                .unwrap();

            // Resolve the last transaction.
            tp_tx
                .send(Transaction::new(TransactionType::Resolve, 1, 2, None))
                .unwrap();

            tp
        };

        let clients = tp.get_results().await.unwrap();
        let client = clients.get(&1).unwrap();

        assert_eq!(client.get_total(), Decimal::new(15, 0)); // We should have all deposited credits.
        assert_eq!(client.is_locked(), false); // The account should not be locked.
    }

    /// Test a scenario where the client will be locked and all subsequent transactions ignored.
    #[tokio::test]
    async fn test_locked_down() {
        let tp = {
            let (tp_tx, tp_rx) = mpsc::unbounded_channel();
            let tp = TransactionProcessor::new(tp_rx);

            // Deposit 10 credits.
            tp_tx
                .send(Transaction::new(
                    TransactionType::Deposit,
                    1,
                    1,
                    Some(Decimal::new(10, 0)),
                ))
                .unwrap();

            // Deposit 5 more.
            tp_tx
                .send(Transaction::new(
                    TransactionType::Deposit,
                    1,
                    2,
                    Some(Decimal::new(5, 0)),
                ))
                .unwrap();

            // Dispute the first deposit (10 credits).
            tp_tx
                .send(Transaction::new(TransactionType::Dispute, 1, 1, None))
                .unwrap();

            // Chargeback the dispute.
            tp_tx
                .send(Transaction::new(TransactionType::Chargeback, 1, 1, None))
                .unwrap();

            // This withdrawal should fail because the client account should be locked by now.
            tp_tx
                .send(Transaction::new(
                    TransactionType::Withdrawal,
                    1,
                    3,
                    Some(Decimal::new(5, 0)),
                ))
                .unwrap();

            tp
        };

        let clients = tp.get_results().await.unwrap();
        let client = clients.get(&1).unwrap();

        assert_eq!(client.get_total(), Decimal::new(5, 0));
        assert_eq!(client.is_locked(), true);
    }
}
