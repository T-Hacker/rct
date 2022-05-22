use anyhow::{bail, Context, Result};
use rust_decimal::Decimal;
use serde::Serialize;

/// Hold the client state.
#[derive(Debug, Serialize)]
pub struct Client {
    id: u16,
    available: Decimal,
    held: Decimal,
    locked: bool,
}

impl Client {
    pub fn new(id: u16) -> Self {
        Self {
            id,
            available: Default::default(),
            held: Default::default(),
            locked: Default::default(),
        }
    }

    pub fn get_id(&self) -> u16 {
        self.id
    }

    pub fn get_available(&self) -> Decimal {
        self.available
    }

    pub fn get_held(&self) -> Decimal {
        self.held
    }

    pub fn get_total(&self) -> Decimal {
        self.available.saturating_add(self.held)
    }

    pub fn is_locked(&self) -> bool {
        self.locked
    }

    pub fn add_available(&mut self, amount: Decimal) -> Result<()> {
        if amount.is_sign_negative() {
            bail!("Amount must be positive.");
        }

        let new_amount = self
            .available
            .checked_add(amount)
            .context("Fail to add to the available founds.")?;

        self.available = new_amount;

        Ok(())
    }

    pub fn subtract_available(&mut self, amount: Decimal) -> Result<()> {
        if amount.is_sign_negative() {
            bail!("Amount must be positive.");
        }

        let new_amount = self
            .available
            .checked_sub(amount)
            .context("Fail to subtract to the available funds.")?;

        if new_amount.is_sign_negative() {
            bail!("Not enough funds available.");
        }

        self.available = new_amount;

        Ok(())
    }

    pub fn transfer_available_to_held(&mut self, amount: Decimal) -> Result<()> {
        if amount.is_sign_negative() {
            bail!("Amount must be positive.");
        }

        let new_available = self
            .available
            .checked_sub(amount)
            .context("Fail to acquire available funds to do the transaction.")?;

        if new_available.is_sign_negative() {
            bail!("Not enough available funds to do the transaction");
        }

        let new_held = self
            .held
            .checked_add(amount)
            .context("Fail to held funds to do the transaction.")?;

        self.available = new_available;
        self.held = new_held;

        Ok(())
    }

    pub fn transfer_held_to_available(&mut self, amount: Decimal) -> Result<()> {
        if amount.is_sign_negative() {
            bail!("Amount must be positive.");
        }

        let new_available = self
            .available
            .checked_add(amount)
            .context("Fail to add funds to available during transaction.")?;

        let new_held = self
            .held
            .checked_sub(amount)
            .context("Fail to subtract from held funds during transaction.")?;

        if new_held.is_sign_negative() {
            bail!("Not enough held funds to the transaction.");
        }

        self.available = new_available;
        self.held = new_held;

        Ok(())
    }

    pub fn subtract_held(&mut self, amount: Decimal) -> Result<()> {
        if amount.is_sign_negative() {
            bail!("Amount must be positive.");
        }

        let new_held = self
            .held
            .checked_sub(amount)
            .context("Fail to subtract from held funds.")?;

        if new_held.is_sign_negative() {
            bail!("Not enough held funds to subtract from.");
        }

        self.held = new_held;

        Ok(())
    }

    pub fn lock_account(&mut self) {
        self.locked = true;
    }
}
