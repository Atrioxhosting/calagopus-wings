fn decimal(value: f64, units: &[&str]) -> String {
    if !value.is_finite() || value <= 0.0 {
        return format!("0 {}", units[0]);
    }

    let index = ((value.log10() / 3.0).floor().max(0.0) as usize).min(units.len() - 1);
    if index == 0 {
        format!("{} {}", value, units[index])
    } else {
        format!(
            "{:.2} {}",
            value / 1000_f64.powi(index as i32),
            units[index]
        )
    }
}

/// Format byte counters for display without altering accounting values.
pub fn bytes(value: f64) -> String {
    decimal(value, &["B", "KB", "MB", "GB", "TB"])
}

/// Convert bytes/s into decimal bits/s for display only.
pub fn rate(bytes_per_second: f64) -> String {
    decimal(
        bytes_per_second * 8.0,
        &["bit/s", "Kbit/s", "Mbit/s", "Gbit/s"],
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimal_network_units() {
        for (input, expected) in [
            (0.0, "0 B"),
            (999.0, "999 B"),
            (1000.0, "1.00 KB"),
            (1024.0, "1.02 KB"),
            (1e6, "1.00 MB"),
            (1e9, "1.00 GB"),
            (1e12, "1.00 TB"),
            (1e15, "1000.00 TB"),
        ] {
            assert_eq!(bytes(input), expected);
        }
    }

    #[test]
    fn byte_rates_become_decimal_bit_rates() {
        for (input, expected) in [
            (0.0, "0 bit/s"),
            (0.125, "1 bit/s"),
            (125.0, "1.00 Kbit/s"),
            (125_000.0, "1.00 Mbit/s"),
            (125_000_000.0, "1.00 Gbit/s"),
            (1e9, "8.00 Gbit/s"),
        ] {
            assert_eq!(rate(input), expected);
        }
    }
}
