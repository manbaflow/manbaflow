use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::MambaApp;
use crate::domain::{ExternalIdentityBinding, Principal, PrincipalKind, Team};
use crate::error::{MambaError, Result};

pub const USER_SCHEMA: &str = "urn:ietf:params:scim:schemas:core:2.0:User";
pub const GROUP_SCHEMA: &str = "urn:ietf:params:scim:schemas:core:2.0:Group";
pub const LIST_SCHEMA: &str = "urn:ietf:params:scim:api:messages:2.0:ListResponse";
pub const PATCH_SCHEMA: &str = "urn:ietf:params:scim:api:messages:2.0:PatchOp";
pub const ERROR_SCHEMA: &str = "urn:ietf:params:scim:api:messages:2.0:Error";
const ACTOR: &str = "tower://scim";

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UserInput {
    #[serde(default)]
    pub schemas: Vec<String>,
    pub user_name: String,
    #[serde(default)]
    pub display_name: Option<String>,
    #[serde(default)]
    pub external_id: Option<String>,
    #[serde(default = "default_active")]
    pub active: bool,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UserResource {
    pub schemas: Vec<String>,
    pub id: String,
    pub external_id: String,
    pub user_name: String,
    pub display_name: String,
    pub active: bool,
    pub groups: Vec<GroupReference>,
    pub meta: ResourceMeta,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupReference {
    pub value: String,
    pub display: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupInput {
    #[serde(default)]
    pub schemas: Vec<String>,
    pub display_name: String,
    #[serde(default)]
    pub external_id: Option<String>,
    #[serde(default)]
    pub members: Vec<MemberInput>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct MemberInput {
    pub value: String,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupResource {
    pub schemas: Vec<String>,
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub external_id: Option<String>,
    pub display_name: String,
    pub members: Vec<MemberResource>,
    pub meta: ResourceMeta,
}

#[derive(Clone, Debug, Serialize)]
pub struct MemberResource {
    pub value: String,
    pub display: String,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceMeta {
    pub resource_type: &'static str,
    pub created: DateTime<Utc>,
    pub last_modified: DateTime<Utc>,
    pub location: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListQuery {
    #[serde(default)]
    pub filter: Option<String>,
    #[serde(default = "default_start_index")]
    pub start_index: usize,
    #[serde(default = "default_count")]
    pub count: usize,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ListResponse<T> {
    pub schemas: Vec<String>,
    pub total_results: usize,
    pub start_index: usize,
    pub items_per_page: usize,
    #[serde(rename = "Resources")]
    pub resources: Vec<T>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PatchRequest {
    #[serde(default)]
    pub schemas: Vec<String>,
    #[serde(rename = "Operations")]
    pub operations: Vec<PatchOperation>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct PatchOperation {
    pub op: String,
    #[serde(default)]
    pub path: Option<String>,
    #[serde(default)]
    pub value: Value,
}

#[derive(Clone, Debug, Serialize)]
pub struct ScimError {
    pub schemas: Vec<String>,
    pub status: String,
    pub detail: String,
    #[serde(rename = "scimType", skip_serializing_if = "Option::is_none")]
    pub scim_type: Option<String>,
}

pub fn list_users(app: &MambaApp, query: &ListQuery) -> Result<ListResponse<UserResource>> {
    validate_page(query)?;
    let filter = query.filter.as_deref().map(parse_filter).transpose()?;
    let mut resources = oidc_bindings(app)
        .into_iter()
        .map(|binding| user_resource(app, binding))
        .collect::<Result<Vec<_>>>()?;
    if let Some((attribute, expected)) = filter {
        resources.retain(|user| match attribute {
            "userName" => user.user_name.eq_ignore_ascii_case(&expected),
            "externalId" => user.external_id == expected,
            "id" => user.id == expected,
            _ => false,
        });
    }
    resources.sort_by(|left, right| left.user_name.cmp(&right.user_name));
    page(resources, query)
}

pub fn get_user(app: &MambaApp, id: &str) -> Result<UserResource> {
    let binding = user_binding(app, id)?;
    user_resource(app, binding)
}

pub fn create_user(app: &mut MambaApp, input: UserInput) -> Result<UserResource> {
    validate_schema(&input.schemas, USER_SCHEMA)?;
    let subject = input.external_id.as_deref().unwrap_or(&input.user_name);
    let name = input.display_name.as_deref().unwrap_or(&input.user_name);
    let (_, binding) =
        app.provision_directory_human(name, &input.user_name, subject, None, input.active, ACTOR)?;
    user_resource(app, &binding)
}

pub fn replace_user(app: &mut MambaApp, id: &str, input: UserInput) -> Result<UserResource> {
    validate_schema(&input.schemas, USER_SCHEMA)?;
    let binding = user_binding(app, id)?.clone();
    if let Some(external_id) = input.external_id.as_deref()
        && external_id != binding.external_user_id
    {
        return Err(MambaError::Validation(
            "SCIM externalId is immutable".into(),
        ));
    }
    let principal = app.state().principal(&binding.principal_id)?.clone();
    let name = input.display_name.as_deref().unwrap_or(&input.user_name);
    app.update_directory_human(
        &principal.id,
        name,
        &input.user_name,
        principal.team_id.as_deref(),
        input.active,
        ACTOR,
    )?;
    get_user(app, id)
}

pub fn patch_user(app: &mut MambaApp, id: &str, patch: PatchRequest) -> Result<UserResource> {
    validate_schema(&patch.schemas, PATCH_SCHEMA)?;
    let binding = user_binding(app, id)?.clone();
    let principal = app.state().principal(&binding.principal_id)?.clone();
    let mut name = principal.name.clone();
    let mut user_name = principal_username(&principal).to_string();
    let mut active = principal.active;
    for operation in patch.operations {
        if !operation.op.eq_ignore_ascii_case("replace") {
            return Err(MambaError::Validation(
                "SCIM User PATCH supports replace operations only".into(),
            ));
        }
        match operation.path.as_deref() {
            Some("active") => active = json_bool(&operation.value, "active")?,
            Some("displayName") => name = json_string(&operation.value, "displayName")?,
            Some("userName") => user_name = json_string(&operation.value, "userName")?,
            None => {
                let object = operation.value.as_object().ok_or_else(|| {
                    MambaError::Validation("SCIM replace value must be an object".into())
                })?;
                if let Some(value) = object.get("active") {
                    active = json_bool(value, "active")?;
                }
                if let Some(value) = object.get("displayName") {
                    name = json_string(value, "displayName")?;
                }
                if let Some(value) = object.get("userName") {
                    user_name = json_string(value, "userName")?;
                }
            }
            Some(path) => {
                return Err(MambaError::Validation(format!(
                    "unsupported SCIM User PATCH path: {path}"
                )));
            }
        }
    }
    app.update_directory_human(
        &principal.id,
        &name,
        &user_name,
        principal.team_id.as_deref(),
        active,
        ACTOR,
    )?;
    get_user(app, id)
}

pub fn delete_user(app: &mut MambaApp, id: &str) -> Result<()> {
    let binding = user_binding(app, id)?.clone();
    let principal = app.state().principal(&binding.principal_id)?.clone();
    app.update_directory_human(
        &principal.id,
        &principal.name,
        principal_username(&principal),
        principal.team_id.as_deref(),
        false,
        ACTOR,
    )?;
    app.unbind_external_identity(&binding.id, ACTOR)?;
    Ok(())
}

pub fn list_groups(app: &MambaApp, query: &ListQuery) -> Result<ListResponse<GroupResource>> {
    validate_page(query)?;
    let filter = query.filter.as_deref().map(parse_filter).transpose()?;
    let mut resources = app
        .state()
        .teams
        .values()
        .filter(|team| team.active)
        .map(|team| group_resource(app, team))
        .collect::<Result<Vec<_>>>()?;
    if let Some((attribute, expected)) = filter {
        resources.retain(|group| match attribute {
            "displayName" => group.display_name.eq_ignore_ascii_case(&expected),
            "externalId" => group.external_id.as_deref() == Some(expected.as_str()),
            "id" => group.id == expected,
            _ => false,
        });
    }
    resources.sort_by(|left, right| left.display_name.cmp(&right.display_name));
    page(resources, query)
}

pub fn get_group(app: &MambaApp, id: &str) -> Result<GroupResource> {
    let team = app.state().team(id)?;
    if !team.active {
        return Err(MambaError::NotFound {
            entity: "SCIM Group",
            id: id.to_string(),
        });
    }
    group_resource(app, team)
}

pub fn create_group(app: &mut MambaApp, input: GroupInput) -> Result<GroupResource> {
    validate_schema(&input.schemas, GROUP_SCHEMA)?;
    validate_members(app, &input.members)?;
    validate_group_external_id(app, None, input.external_id.as_deref())?;
    let team = app.create_team(&input.display_name, "", ACTOR)?;
    let team = app.update_directory_team(
        &team.id,
        &team.name,
        input.external_id.as_deref(),
        true,
        ACTOR,
    )?;
    reconcile_members(app, &team.id, &input.members)?;
    get_group(app, &team.id)
}

pub fn replace_group(app: &mut MambaApp, id: &str, input: GroupInput) -> Result<GroupResource> {
    validate_schema(&input.schemas, GROUP_SCHEMA)?;
    validate_members(app, &input.members)?;
    let current = app.state().team(id)?.clone();
    if input.external_id.is_some() && input.external_id != current.directory_external_id {
        return Err(MambaError::Validation(
            "SCIM Group externalId is immutable".into(),
        ));
    }
    let team = app.update_directory_team(
        id,
        &input.display_name,
        current.directory_external_id.as_deref(),
        true,
        ACTOR,
    )?;
    reconcile_members(app, &team.id, &input.members)?;
    get_group(app, &team.id)
}

pub fn patch_group(app: &mut MambaApp, id: &str, patch: PatchRequest) -> Result<GroupResource> {
    validate_schema(&patch.schemas, PATCH_SCHEMA)?;
    let mut team = app.state().team(id)?.clone();
    for operation in patch.operations {
        match (
            operation.op.to_ascii_lowercase().as_str(),
            operation.path.as_deref(),
        ) {
            ("replace", Some("displayName")) => {
                let name = json_string(&operation.value, "displayName")?;
                team = app.update_directory_team(
                    &team.id,
                    &name,
                    team.directory_external_id.as_deref(),
                    team.active,
                    ACTOR,
                )?;
            }
            ("replace", Some("active")) => {
                let active = json_bool(&operation.value, "active")?;
                team = app.update_directory_team(
                    &team.id,
                    &team.name,
                    team.directory_external_id.as_deref(),
                    active,
                    ACTOR,
                )?;
            }
            ("add", Some("members")) => {
                assign_members(app, &team.id, member_values(&operation.value)?)?;
            }
            ("remove", Some("members")) => {
                remove_members(app, &team.id, member_values(&operation.value)?)?;
            }
            ("remove", Some(path)) if path.starts_with("members[") => {
                remove_members(app, &team.id, vec![member_from_path(path)?])?;
            }
            (_, path) => {
                return Err(MambaError::Validation(format!(
                    "unsupported SCIM Group PATCH operation/path: {} {}",
                    operation.op,
                    path.unwrap_or("<none>")
                )));
            }
        }
    }
    get_group(app, &team.id)
}

pub fn delete_group(app: &mut MambaApp, id: &str) -> Result<()> {
    let team = app.state().team(id)?.clone();
    let members = app
        .state()
        .principals
        .values()
        .filter(|principal| principal.team_id.as_deref() == Some(&team.id))
        .map(|principal| principal.id.clone())
        .collect::<Vec<_>>();
    for member in members {
        let principal = app.state().principal(&member)?.clone();
        app.update_directory_human(
            &member,
            &principal.name,
            principal_username(&principal),
            None,
            principal.active,
            ACTOR,
        )?;
    }
    app.update_directory_team(&team.id, &team.name, None, false, ACTOR)?;
    Ok(())
}

pub fn error(status: u16, detail: impl Into<String>, scim_type: Option<&str>) -> ScimError {
    ScimError {
        schemas: vec![ERROR_SCHEMA.into()],
        status: status.to_string(),
        detail: detail.into(),
        scim_type: scim_type.map(str::to_string),
    }
}

fn user_resource(app: &MambaApp, binding: &ExternalIdentityBinding) -> Result<UserResource> {
    let principal = app.state().principal(&binding.principal_id)?;
    let groups = principal
        .team_id
        .as_deref()
        .map(|team_id| app.state().team(team_id))
        .transpose()?
        .map(|team| {
            vec![GroupReference {
                value: team.id.clone(),
                display: team.name.clone(),
            }]
        })
        .unwrap_or_default();
    Ok(UserResource {
        schemas: vec![USER_SCHEMA.into()],
        id: binding.id.clone(),
        external_id: binding.external_user_id.clone(),
        user_name: principal_username(principal).to_string(),
        display_name: principal.name.clone(),
        active: principal.active,
        groups,
        meta: ResourceMeta {
            resource_type: "User",
            created: principal.created_at,
            last_modified: binding.bound_at,
            location: format!("/scim/v2/Users/{}", binding.id),
        },
    })
}

fn group_resource(app: &MambaApp, team: &Team) -> Result<GroupResource> {
    let members = app
        .state()
        .principals
        .values()
        .filter(|principal| {
            principal.kind == PrincipalKind::Human && principal.team_id.as_deref() == Some(&team.id)
        })
        .filter_map(|principal| {
            oidc_bindings(app)
                .into_iter()
                .find(|binding| binding.principal_id == principal.id)
                .map(|binding| MemberResource {
                    value: binding.id.clone(),
                    display: principal.name.clone(),
                })
        })
        .collect();
    Ok(GroupResource {
        schemas: vec![GROUP_SCHEMA.into()],
        id: team.id.clone(),
        external_id: team.directory_external_id.clone(),
        display_name: team.name.clone(),
        members,
        meta: ResourceMeta {
            resource_type: "Group",
            created: team.created_at,
            last_modified: team.created_at,
            location: format!("/scim/v2/Groups/{}", team.id),
        },
    })
}

fn oidc_bindings(app: &MambaApp) -> Vec<&ExternalIdentityBinding> {
    app.state()
        .external_identities
        .values()
        .filter(|binding| binding.provider == "oidc" && binding.is_active())
        .collect()
}

fn user_binding<'a>(app: &'a MambaApp, id: &str) -> Result<&'a ExternalIdentityBinding> {
    app.state()
        .external_identities
        .values()
        .find(|binding| binding.id == id && binding.provider == "oidc" && binding.is_active())
        .ok_or_else(|| MambaError::NotFound {
            entity: "SCIM User",
            id: id.to_string(),
        })
}

fn reconcile_members(app: &mut MambaApp, team_id: &str, members: &[MemberInput]) -> Result<()> {
    let desired = members
        .iter()
        .map(|member| member.value.clone())
        .collect::<Vec<_>>();
    let current = app
        .state()
        .principals
        .values()
        .filter(|principal| principal.team_id.as_deref() == Some(team_id))
        .filter_map(|principal| {
            oidc_bindings(app)
                .into_iter()
                .find(|binding| binding.principal_id == principal.id)
                .map(|binding| binding.id.clone())
        })
        .collect::<Vec<_>>();
    let removed = current
        .into_iter()
        .filter(|member| !desired.contains(member))
        .collect::<Vec<_>>();
    remove_members(app, team_id, removed)?;
    assign_members(app, team_id, desired)
}

fn validate_members(app: &MambaApp, members: &[MemberInput]) -> Result<()> {
    let mut seen = std::collections::BTreeSet::new();
    for member in members {
        if !seen.insert(member.value.as_str()) {
            return Err(MambaError::Validation(format!(
                "duplicate SCIM Group member: {}",
                member.value
            )));
        }
        user_binding(app, &member.value)?;
    }
    Ok(())
}

fn validate_group_external_id(
    app: &MambaApp,
    group_id: Option<&str>,
    external_id: Option<&str>,
) -> Result<()> {
    if external_id.is_some_and(|external_id| {
        app.state().teams.values().any(|team| {
            Some(team.id.as_str()) != group_id
                && team.directory_external_id.as_deref() == Some(external_id)
        })
    }) {
        return Err(MambaError::Validation(
            "directory Group externalId already exists".into(),
        ));
    }
    Ok(())
}

fn assign_members(app: &mut MambaApp, team_id: &str, members: Vec<String>) -> Result<()> {
    let principals = members
        .iter()
        .map(|member| {
            let binding = user_binding(app, member)?;
            Ok(app.state().principal(&binding.principal_id)?.clone())
        })
        .collect::<Result<Vec<_>>>()?;
    for principal in principals {
        app.update_directory_human(
            &principal.id,
            &principal.name,
            principal_username(&principal),
            Some(team_id),
            principal.active,
            ACTOR,
        )?;
    }
    Ok(())
}

fn remove_members(app: &mut MambaApp, team_id: &str, members: Vec<String>) -> Result<()> {
    let principals = members
        .iter()
        .map(|member| {
            let binding = user_binding(app, member)?;
            Ok(app.state().principal(&binding.principal_id)?.clone())
        })
        .collect::<Result<Vec<_>>>()?;
    for principal in principals {
        if principal.team_id.as_deref() == Some(team_id) {
            app.update_directory_human(
                &principal.id,
                &principal.name,
                principal_username(&principal),
                None,
                principal.active,
                ACTOR,
            )?;
        }
    }
    Ok(())
}

fn member_values(value: &Value) -> Result<Vec<String>> {
    let values = if value.is_array() {
        value.as_array().cloned().unwrap_or_default()
    } else {
        vec![value.clone()]
    };
    values
        .into_iter()
        .map(|value| {
            value
                .get("value")
                .and_then(Value::as_str)
                .map(str::to_string)
                .ok_or_else(|| MambaError::Validation("SCIM member value is required".into()))
        })
        .collect()
}

fn member_from_path(path: &str) -> Result<String> {
    let expression = path
        .strip_prefix("members[")
        .and_then(|value| value.strip_suffix(']'))
        .ok_or_else(|| MambaError::Validation("invalid SCIM member filter path".into()))?;
    let (attribute, value) = parse_filter(expression)?;
    if attribute != "value" {
        return Err(MambaError::Validation(
            "SCIM member filter must target value".into(),
        ));
    }
    Ok(value)
}

fn principal_username(principal: &Principal) -> &str {
    principal
        .directory_username
        .as_deref()
        .unwrap_or(&principal.name)
}

fn page<T>(resources: Vec<T>, query: &ListQuery) -> Result<ListResponse<T>> {
    let total_results = resources.len();
    let offset = query.start_index.saturating_sub(1).min(total_results);
    let resources = resources
        .into_iter()
        .skip(offset)
        .take(query.count)
        .collect::<Vec<_>>();
    Ok(ListResponse {
        schemas: vec![LIST_SCHEMA.into()],
        total_results,
        start_index: query.start_index,
        items_per_page: resources.len(),
        resources,
    })
}

fn parse_filter(value: &str) -> Result<(&str, String)> {
    let mut parts = value.trim().splitn(3, char::is_whitespace);
    let attribute = parts.next().unwrap_or_default();
    let operator = parts.next().unwrap_or_default();
    let expected = parts.next().unwrap_or_default().trim();
    if !operator.eq_ignore_ascii_case("eq")
        || !expected.starts_with('"')
        || !expected.ends_with('"')
        || expected.len() < 2
    {
        return Err(MambaError::Validation(
            "SCIM filter must use attribute eq \"value\"".into(),
        ));
    }
    if !matches!(
        attribute,
        "userName" | "externalId" | "displayName" | "id" | "value"
    ) {
        return Err(MambaError::Validation(format!(
            "unsupported SCIM filter attribute: {attribute}"
        )));
    }
    Ok((attribute, expected[1..expected.len() - 1].to_string()))
}

fn validate_page(query: &ListQuery) -> Result<()> {
    if query.start_index == 0 || query.count == 0 || query.count > 200 {
        return Err(MambaError::Validation(
            "SCIM startIndex must be positive and count must be between 1 and 200".into(),
        ));
    }
    Ok(())
}

fn validate_schema(schemas: &[String], expected: &str) -> Result<()> {
    if !schemas.is_empty() && !schemas.iter().any(|schema| schema == expected) {
        return Err(MambaError::Validation(format!(
            "SCIM request is missing schema {expected}"
        )));
    }
    Ok(())
}

fn json_bool(value: &Value, name: &str) -> Result<bool> {
    value
        .as_bool()
        .ok_or_else(|| MambaError::Validation(format!("SCIM {name} must be boolean")))
}

fn json_string(value: &Value, name: &str) -> Result<String> {
    value
        .as_str()
        .map(str::to_string)
        .ok_or_else(|| MambaError::Validation(format!("SCIM {name} must be a string")))
}

fn default_active() -> bool {
    true
}

fn default_start_index() -> usize {
    1
}

fn default_count() -> usize {
    100
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn scim_users_and_groups_drive_principal_lifecycle_and_access() {
        let directory = tempdir().unwrap();
        let mut app = MambaApp::open(directory.path()).unwrap();
        app.init_organization("Mamba", "admin").unwrap();
        let user = create_user(
            &mut app,
            UserInput {
                schemas: vec![USER_SCHEMA.into()],
                user_name: "zobayan@example.com".into(),
                display_name: Some("Zobayan".into()),
                external_id: Some("oidc-subject-7".into()),
                active: true,
            },
        )
        .unwrap();
        let group = create_group(
            &mut app,
            GroupInput {
                schemas: vec![GROUP_SCHEMA.into()],
                display_name: "Platform".into(),
                external_id: Some("idp-group-1".into()),
                members: vec![MemberInput {
                    value: user.id.clone(),
                }],
            },
        )
        .unwrap();
        let duplicate_user = create_user(
            &mut app,
            UserInput {
                schemas: vec![USER_SCHEMA.into()],
                user_name: "zobayan@example.com".into(),
                display_name: Some("Another Pilot".into()),
                external_id: Some("oidc-subject-8".into()),
                active: true,
            },
        )
        .unwrap_err();
        assert!(duplicate_user.to_string().contains("userName"));
        let duplicate_group = create_group(
            &mut app,
            GroupInput {
                schemas: vec![GROUP_SCHEMA.into()],
                display_name: "Aviation".into(),
                external_id: Some("idp-group-1".into()),
                members: Vec::new(),
            },
        )
        .unwrap_err();
        assert!(duplicate_group.to_string().contains("externalId"));
        assert!(
            app.state()
                .teams
                .values()
                .all(|team| team.name != "Aviation")
        );
        assert_eq!(get_user(&app, &user.id).unwrap().groups[0].value, group.id);
        patch_group(
            &mut app,
            &group.id,
            PatchRequest {
                schemas: vec![PATCH_SCHEMA.into()],
                operations: vec![PatchOperation {
                    op: "remove".into(),
                    path: Some(format!("members[value eq \"{}\"]", user.id)),
                    value: Value::Null,
                }],
            },
        )
        .unwrap();
        assert!(get_group(&app, &group.id).unwrap().members.is_empty());
        patch_group(
            &mut app,
            &group.id,
            PatchRequest {
                schemas: vec![PATCH_SCHEMA.into()],
                operations: vec![PatchOperation {
                    op: "add".into(),
                    path: Some("members".into()),
                    value: serde_json::json!([{"value": user.id}]),
                }],
            },
        )
        .unwrap();
        let principal = app.oidc_principal("oidc-subject-7").unwrap();
        let session = app.issue_oidc_session(&principal.id).unwrap();
        assert!(
            app.authenticate_api_token(&session.token)
                .unwrap()
                .is_some()
        );

        patch_user(
            &mut app,
            &user.id,
            PatchRequest {
                schemas: vec![PATCH_SCHEMA.into()],
                operations: vec![PatchOperation {
                    op: "replace".into(),
                    path: Some("active".into()),
                    value: Value::Bool(false),
                }],
            },
        )
        .unwrap();
        assert!(
            app.authenticate_api_token(&session.token)
                .unwrap()
                .is_none()
        );
        assert!(app.oidc_principal("oidc-subject-7").is_err());

        delete_group(&mut app, &group.id).unwrap();
        assert!(get_group(&app, &group.id).is_err());
        delete_user(&mut app, &user.id).unwrap();
        assert!(get_user(&app, &user.id).is_err());

        let replayed = MambaApp::open(directory.path()).unwrap();
        assert!(get_user(&replayed, &user.id).is_err());
        assert!(get_group(&replayed, &group.id).is_err());
    }
}
