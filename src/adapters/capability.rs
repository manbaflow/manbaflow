use std::path::Path;

use crate::domain::{CapabilityPack, FlightDeliverable, OutputContract};

pub struct CapabilityAdapter;

impl CapabilityAdapter {
    pub fn execution_directive(pack: CapabilityPack) -> &'static str {
        match pack {
            CapabilityPack::Coding => {
                "Implement the scoped code change, run relevant checks, and leave source changes in the isolated worktree. Do not push, merge, change remotes, or access credentials."
            }
            CapabilityPack::Office => {
                "Create or edit only the declared office deliverables in the isolated worktree. Do not send email, invite attendees, publish, upload to cloud storage, or use external credentials. Produce reviewable drafts for Human release."
            }
            CapabilityPack::General => {
                "Produce the scoped deliverable inside the isolated worktree. Do not push, publish, send externally, change remotes, or access credentials."
            }
        }
    }

    pub fn derive_deliverables(
        changed_files: &[String],
        requires_human_release: bool,
    ) -> Vec<FlightDeliverable> {
        changed_files
            .iter()
            .cloned()
            .map(|path| FlightDeliverable::from_path(path, requires_human_release))
            .collect()
    }

    pub fn contract_violations(
        contract: &OutputContract,
        deliverables: &[FlightDeliverable],
    ) -> Vec<String> {
        let mut violations = Vec::new();
        if deliverables.len() < contract.min_deliverables as usize {
            violations.push(format!(
                "{} deliverable(s) produced; at least {} required",
                deliverables.len(),
                contract.min_deliverables
            ));
        }
        if !contract.allowed_extensions.is_empty() {
            for deliverable in deliverables {
                let extension = Path::new(&deliverable.path)
                    .extension()
                    .and_then(|value| value.to_str())
                    .map(str::to_ascii_lowercase);
                if !extension.as_ref().is_some_and(|extension| {
                    contract
                        .allowed_extensions
                        .iter()
                        .any(|allowed| allowed.eq_ignore_ascii_case(extension))
                }) {
                    violations.push(format!(
                        "deliverable {} is outside the extension allowlist",
                        deliverable.path
                    ));
                }
            }
        }
        violations
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{DeliverableKind, OutputContract};

    #[test]
    fn office_adapter_classifies_outputs_and_rejects_source_files() {
        let contract = OutputContract::for_pack(CapabilityPack::Office);
        let deliverables = CapabilityAdapter::derive_deliverables(
            &["brief.docx".into(), "forecast.xlsx".into()],
            true,
        );
        assert_eq!(deliverables[0].kind, DeliverableKind::Document);
        assert_eq!(deliverables[1].kind, DeliverableKind::Spreadsheet);
        assert!(CapabilityAdapter::contract_violations(&contract, &deliverables).is_empty());

        let source = CapabilityAdapter::derive_deliverables(&["src/main.rs".into()], true);
        assert_eq!(source[0].kind, DeliverableKind::Code);
        assert_eq!(
            CapabilityAdapter::contract_violations(&contract, &source).len(),
            1
        );
        assert!(
            CapabilityAdapter::execution_directive(CapabilityPack::Office)
                .contains("Do not send email")
        );
    }
}
