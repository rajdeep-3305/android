# NetworkBonding for Android (android_packages_apps_NetworkBonding)

**Bond Wi-Fi and mobile data simultaneously for maximum aggregate bandwidth and sub-second failover.**
No root required, no external proxy/relay server, 100% on-device local engine.

## Key Features
- **4-Mode Dynamic Scheduler:**
  - **Turbo Boost (Max Speed):** Proportional flow scheduling based on real-time EWMA throughput capacity tracking ($\alpha = 0.35$ at 4 Hz) to fully saturate asymmetric links (e.g. 14 Mbps Wi-Fi + 65 Mbps Cellular).
  - **Smart Adaptive (QoS):** Automatic latency steering directing DNS, Gaming (UDP/TCP), VoIP, and SSH to the lowest-ping link, while bonding bulk transfers.
  - **Data Saver:** Uses Wi-Fi exclusively to save cellular data, failing over seamlessly if Wi-Fi degrades.
  - **Split Evenly:** Symmetrical 50/50 connection distribution for links with equal throughput.
- **Sub-Second Circuit Breaker:** Port 53 / 443 active health probes with 1200ms timeout to catch captive portals and dead connections instantly.
- **16 KB Page Aligned:** Native libraries linked with `-Wl,-z,max-page-size=16384` for modern Android 15/16 kernels across Qualcomm, MediaTek, Exynos, and Tensor platforms.
- **Native AOSP Integration:** Cleanly drops into `packages/apps/Settings` and AOSP manifests as `android_packages_apps_NetworkBonding`.
- **Per-App Bypass:** Exclude banking, streaming, and local network apps from the bonded tunnel.

## Building from source

Requirements: Go 1.27+, JDK 17+, Android SDK, and Android NDK (r27c+).

```sh
export ANDROID_HOME=~/Android/Sdk
export ANDROID_NDK_HOME=$ANDROID_HOME/ndk/<version>
./scripts/build-go.sh
```

Run unit and race-detector tests:
```sh
cd golib && go test -race -v ./...
```

## License
MIT Licensed. Based on the upstream net-merge engine by Rohit Mahto.
