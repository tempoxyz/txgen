/// Deep-merge a YAML overlay into a base value.
///
/// Mapping values are merged recursively. All other overlay values replace the
/// base value. A `null` overlay is treated as no-op so omitted `with` blocks do
/// not erase the referenced template.
pub fn merge_yaml(base: &mut serde_yaml::Value, overlay: serde_yaml::Value) {
    if matches!(overlay, serde_yaml::Value::Null) {
        return;
    }

    match (base, overlay) {
        (serde_yaml::Value::Mapping(base_map), serde_yaml::Value::Mapping(overlay_map)) => {
            for (key, value) in overlay_map {
                match base_map.get_mut(&key) {
                    Some(base_value) => merge_yaml(base_value, value),
                    None => {
                        base_map.insert(key, value);
                    }
                }
            }
        }
        (base_value, overlay_value) => {
            *base_value = overlay_value;
        }
    }
}
