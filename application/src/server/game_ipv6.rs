//! Native game IPv6 uses one Docker endpoint and the existing eth0 accounting boundary.
use super::configuration::GameIpv6;
use std::{collections::HashMap, net::Ipv6Addr};

#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct Endpoint {
    pub id: String,
    pub running: bool,
    pub addresses: Vec<String>,
}

pub fn prefix(value: &str) -> Result<u128, anyhow::Error> {
    let address = value
        .strip_suffix("/64")
        .ok_or_else(|| anyhow::anyhow!("Game prefix must be /64"))?;
    let address: Ipv6Addr = address.parse()?;
    let bits = u128::from(address);
    anyhow::ensure!(
        bits >> 125 == 1 && bits as u64 == 0,
        "Game prefix must be a canonical global-unicast /64"
    );
    Ok(bits)
}

pub fn validate(config: &GameIpv6) -> Result<(), anyhow::Error> {
    let network = prefix(&config.prefix)?;
    let address: Ipv6Addr = config.address.parse()?;
    let bits = u128::from(address);
    anyhow::ensure!(
        bits >> 64 == network >> 64,
        "IPv6 address is outside the game prefix"
    );
    anyhow::ensure!(
        bits as u64 > 1 && bits as u64 != u64::MAX - 1,
        "Reserved game IPv6 address"
    );
    Ok(())
}

pub async fn ensure_network(
    docker: &bollard::Docker,
    config: &crate::config::Config,
    name: &str,
    subnet: &str,
) -> Result<(), anyhow::Error> {
    let network = prefix(subnet)?;
    let existing = docker.inspect_network(name, None).await;
    if let Err(bollard::errors::Error::DockerResponseServerError {
        status_code: 404, ..
    }) = &existing
    {
        let settings = config.load();
        anyhow::ensure!(
            settings.docker.network.driver == "bridge",
            "Native game IPv6 requires a bridge network"
        );
        let options = HashMap::from([
            (
                "com.docker.network.bridge.gateway_mode_ipv6".into(),
                "routed".into(),
            ),
            (
                "com.docker.network.bridge.enable_ip_masquerade".into(),
                "true".into(),
            ),
            (
                "com.docker.network.bridge.enable_icc".into(),
                "false".into(),
            ),
            (
                "com.docker.network.driver.mtu".into(),
                settings.docker.network.network_mtu.to_string(),
            ),
        ]);
        drop(settings);
        // Docker allocates a non-overlapping private IPv4 pool. Do not reuse the legacy ULA bridge pool.
        let request = bollard::models::NetworkCreateRequest {
            name: name.into(),
            driver: Some("bridge".into()),
            enable_ipv4: Some(true),
            enable_ipv6: Some(true),
            ipam: Some(bollard::models::Ipam {
                config: Some(vec![bollard::models::IpamConfig {
                    subnet: Some(subnet.into()),
                    gateway: Some(Ipv6Addr::from(network | u128::from(u64::MAX - 1)).to_string()),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            options: Some(options),
            ..Default::default()
        };
        if let Err(error) = docker.create_network(request).await {
            // Another server may have created this node-wide network concurrently.
            if docker.inspect_network(name, None).await.is_err() {
                return Err(error.into());
            }
        }
    } else {
        existing?;
    }
    let actual = docker.inspect_network(name, None).await?;
    anyhow::ensure!(
        actual.driver.as_deref() == Some("bridge") && actual.enable_ipv4 == Some(true),
        "Game network must be a dual-stack Docker bridge"
    );
    anyhow::ensure!(
        actual.enable_ipv6 == Some(true),
        "Game bridge does not enable IPv6"
    );
    anyhow::ensure!(
        actual
            .options
            .as_ref()
            .and_then(|o| o.get("com.docker.network.bridge.gateway_mode_ipv6"))
            .map(String::as_str)
            == Some("routed"),
        "Game bridge must use routed IPv6; refusing NAT66"
    );
    for (option, expected) in [
        ("com.docker.network.bridge.enable_icc", "false"),
        ("com.docker.network.bridge.enable_ip_masquerade", "true"),
    ] {
        anyhow::ensure!(
            actual
                .options
                .as_ref()
                .and_then(|options| options.get(option))
                .map(String::as_str)
                == Some(expected),
            "Game bridge has an incompatible {option} setting"
        );
    }
    anyhow::ensure!(
        actual
            .ipam
            .and_then(|i| i.config)
            .unwrap_or_default()
            .iter()
            .any(|i| i.subnet.as_deref() == Some(subnet)
                && i.gateway.as_deref()
                    == Some(
                        Ipv6Addr::from(network | u128::from(u64::MAX - 1))
                            .to_string()
                            .as_str()
                    )),
        "Existing game bridge has a different prefix; explicit migration required"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn validates_global_prefix_and_reserved_addresses() {
        let make = |address: &str| GameIpv6 {
            address: address.into(),
            prefix: "2a0c:b641:620:1000::/64".into(),
        };
        assert!(validate(&make("2a0c:b641:620:1000::2")).is_ok());
        for address in [
            "2a0c:b641:620:1000::",
            "2a0c:b641:620:1000::1",
            "2a0c:b641:620:1000:ffff:ffff:ffff:fffe",
            "2a0c:b641:620:1001::2",
            "::1",
        ] {
            assert!(validate(&make(address)).is_err(), "{address}");
        }
        for value in [
            "fd00::/64",
            "2a0c:b641:620:1000::2/64",
            "2a0c:b641:620:1000::/48",
        ] {
            assert!(prefix(value).is_err());
        }
    }
}
