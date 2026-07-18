# 第三方软件声明

飞行桥本身依据 Apache License 2.0 发布。项目没有把以下 Python 依赖的源码复制进本仓库；安装工具会从 Python 软件源分别安装它们，各依赖继续适用自己的许可证。

## 直接运行时依赖

| 组件 | 许可证 | 用途 |
| --- | --- | --- |
| [httpx](https://github.com/encode/httpx) | BSD-3-Clause | 飞书 HTTP API |
| [lark-oapi](https://github.com/larksuite/oapi-sdk-python) | MIT | 飞书 OpenAPI 与 WebSocket SDK |
| [Pillow](https://github.com/python-pillow/Pillow) | MIT-CMU | 图片安全重编码 |

## 当前解析出的传递依赖

| 组件 | 许可证 |
| --- | --- |
| anyio | MIT |
| certifi | MPL-2.0 |
| charset-normalizer | MIT |
| h11 | MIT |
| httpcore | BSD-3-Clause |
| idna | BSD-3-Clause |
| pycryptodome | BSD / Public Domain |
| requests | Apache-2.0 |
| requests-toolbelt | Apache-2.0 |
| typing-extensions | PSF-2.0 |
| urllib3 | MIT |
| websockets | BSD-3-Clause |

准确版本以安装时生成的环境或发布构件 SBOM 为准。依赖发行包通常包含其完整许可证文本；重新分发依赖二进制或源码时，分发者仍需保留相应许可证及声明。

## 宣传素材

仓库中的演示图只使用经匿名化处理的项目截图和项目自有的中性示例内容。旧的真实头像、昵称、主机位置以及来源不明的卡牌画面不属于发布内容。
