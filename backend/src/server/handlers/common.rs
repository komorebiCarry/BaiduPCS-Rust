use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
pub struct BatchOperationRequest {
    pub task_ids: Option<Vec<String>>,
    #[serde(default)]
    pub all: Option<bool>,
    #[serde(default)]
    pub delete_files: Option<bool>,
}

#[derive(Debug, Serialize)]
pub struct BatchOperationItem {
    pub task_id: String,
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct BatchOperationResponse {
    pub total: usize,
    pub success_count: usize,
    pub failed_count: usize,
    pub results: Vec<BatchOperationItem>,
}

impl BatchOperationResponse {
    pub fn from_results(results: Vec<BatchOperationItem>) -> Self {
        let total = results.len();
        let success_count = results.iter().filter(|r| r.success).count();
        Self {
            total,
            success_count,
            failed_count: total - success_count,
            results,
        }
    }
}
