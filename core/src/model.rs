use rkyv::{Archive, Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq, Clone)]
#[archive(check_bytes)] // Enables bytecheck validation for zero-copy safety
pub struct Node {
    pub id: u64,
    pub embedding: Vec<f32>,
    pub data: String, // Raw text or JSON content
    pub metadata: HashMap<String, String>,
}

#[derive(Archive, Deserialize, Serialize, Debug, PartialEq, Clone)]
#[archive(check_bytes)]
pub struct Edge {
    pub source: u64,
    pub target: u64,
    pub relation: String,
    pub weight: f32,
    pub metadata: HashMap<String, String>,
}

impl Node {
    pub fn new(id: u64, embedding: Vec<f32>, data: String) -> Self {
        Self {
            id,
            embedding,
            data,
            metadata: HashMap::new(),
        }
    }
}

impl Edge {
    pub fn new(source: u64, target: u64, relation: impl Into<String>, weight: f32) -> Self {
        Self {
            source,
            target,
            relation: relation.into(),
            weight,
            metadata: HashMap::new(),
        }
    }
}
