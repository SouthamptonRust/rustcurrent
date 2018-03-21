#![allow(dead_code)]
//! A lock-free concurrency library for Rust.
//!
//! This crate provides both some lock-free data structures and lock-free memory management.
//! The structures can be used in a variety of scenarios. Of particular interest is the wait-free
//! HashMap, which guarantees that progress is made by every thread in a bounded amount of time.

extern crate time;
extern crate rand;
extern crate thread_local;

pub mod structures;
pub mod memory;

mod tests {
   
}
