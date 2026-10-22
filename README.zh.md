# paseo-relay

中文 | [English](README.md)

非常轻量的自部署 [Paseo](https://github.com/getpaseo/paseo) 中继服务器。

当 Paseo 守护进程和移动端无法直连时，中继服务器负责桥接两者。它只转发加密字节，无法读取内容 —— 中继服务器在设计上完全不可信。

## 工作原理

```
移动端  ──────────────────────────────────────┐
                                              ▼
                                      [ paseo-relay ]
                                              ▲
Paseo 守护进程 ────────────────────────────────┘
```

守护进程与移动端之间的所有流量均采用 XSalsa20-Poly1305（NaCl box）端到端加密。中继服务器只能看到 IP 地址、时序、消息大小和会话 ID，无法获取任何内容。

## 协议（v2）

`GET /ws` 下有三个 WebSocket 端点：

| 角色 | 查询参数 | 说明 |
|------|---------|------|
| 守护进程控制 | `role=server&v=2` | 每个守护进程会话一个，接收连接/断开事件 |
| 守护进程数据 | `role=server&connectionId=<c>&v=2` | 每个客户端连接一个，转发加密帧 |
| 客户端 | `role=client[&connectionId=<c>]&v=2` | 移动端 socket，每个 connectionId 允许多个 |

`GET /health` 返回 `{"status":"ok","version":"<version>"}`。

## 快速开始

### Docker

```bash
docker run -p 8411:8411 ghcr.io/zenghongtu/paseo-relay:latest
```

### 预编译二进制

从 [Releases](https://github.com/zenghongtu/paseo-relay/releases) 下载对应平台的二进制文件：

| 平台 | 文件 |
|------|------|
| Linux x86_64 | `paseo-relay-<version>-linux-amd64.tar.gz` |
| Linux ARM64 | `paseo-relay-<version>-linux-arm64.tar.gz` |
| macOS x86_64 | `paseo-relay-<version>-darwin-amd64.tar.gz` |
| macOS Apple Silicon | `paseo-relay-<version>-darwin-arm64.tar.gz` |
| Windows x86_64 | `paseo-relay-<version>-windows-amd64.zip` |

```bash
# Linux/macOS
curl -L https://github.com/zenghongtu/paseo-relay/releases/latest/download/paseo-relay-v0.3.0-linux-amd64.tar.gz | tar xz
./paseo-relay

# Windows：解压 .zip 后运行 paseo-relay.exe
```

### 从源码构建

```bash
git clone https://github.com/zenghongtu/paseo-relay
cd paseo-relay
go build -o paseo-relay .
./paseo-relay
```

## 配置

| 参数 | 环境变量 | 默认值 | 说明 |
|------|---------|--------|------|
| `--addr` | `RELAY_ADDR` | `:8411` | 监听地址 |
| `--max-buffer-frames` | — | `200` | 守护进程连接期间每个连接最多缓冲的帧数 |
| `--max-message-memory` | — | `256` | 所有连接合计在内存中持有的消息数据上限（MiB）。用满后拒收新消息，发送方可能被以 1013 断开。`0` 表示不限制 |
| `--log-format` | `LOG_FORMAT` | `text` | 日志格式：`text` 或 `json` |
| `--version` | — | — | 打印版本并退出 |

## 部署

### systemd

```ini
[Unit]
Description=Paseo relay
After=network.target

[Service]
ExecStart=/usr/local/bin/paseo-relay --addr :8411 --log-format json
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

### Docker Compose

```yaml
services:
  relay:
    image: ghcr.io/zenghongtu/paseo-relay:latest
    restart: unless-stopped
    ports:
      - "8411:8411"
    environment:
      LOG_FORMAT: json
```

TLS 应在上游终止（nginx、Caddy、Cloudflare 等）。

### 配置 Paseo 守护进程

编辑 `~/.paseo/config.json`：

**使用域名：**
```json
{
  "daemon": {
    "relay": {
      "enabled": true,
      "endpoint": "relay.yourdomain.com:443",
      "publicEndpoint": "relay.yourdomain.com:443",
      "useTls": true
    }
  }
}
```

**使用 IP 地址：**
```json
{
  "daemon": {
    "relay": {
      "enabled": true,
      "endpoint": "1.2.3.4:8411",
      "publicEndpoint": "1.2.3.4:8411"
    }
  }
}
```

然后重启守护进程：`paseo daemon stop && paseo daemon start`。

## 安全

- 中继服务器是零知识的 —— 无法读取或伪造消息。
- 中继本身不做鉴权；安全性由 Paseo 的 ECDH + NaCl box 端到端加密保证。
- 务必在上游终止 TLS，不要通过明文 HTTP 暴露中继。
- 转发消息占用的内存受 `--max-message-memory` 限制。高负载时整个进程仍可能用到它的数倍（测试中最高约 4 倍）；可设置 `GOMEMLIMIT`（例如 `512MiB`）让占用更贴近该值。
- 完整威胁模型请参见 [Paseo SECURITY.md](https://github.com/getpaseo/paseo/blob/main/SECURITY.md)。

## 许可证

AGPLv3 —— 详见 [LICENSE](LICENSE)。
