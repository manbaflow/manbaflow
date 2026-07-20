use uuid::Uuid;

pub fn new_id(prefix: &str) -> String {
    let value = Uuid::new_v4().simple().to_string();
    format!("{prefix}-{}", &value[..8])
}

pub fn normalize_capability(value: &str) -> String {
    value.trim().to_lowercase().replace([' ', '_'], "-")
}

pub fn parse_capabilities(values: impl IntoIterator<Item = String>) -> Vec<String> {
    let mut result: Vec<String> = values
        .into_iter()
        .flat_map(|value| {
            value
                .split(',')
                .map(normalize_capability)
                .filter(|value| !value.is_empty())
                .collect::<Vec<_>>()
        })
        .collect();
    result.sort();
    result.dedup();
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capabilities_are_normalized_and_deduplicated() {
        assert_eq!(
            parse_capabilities(["Rust, Code Review".to_string(), "rust".to_string()]),
            vec!["code-review", "rust"]
        );
    }
}
