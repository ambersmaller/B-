# Changelog

所有重要变更都会记录在本文件中。

## [2.2.4] - 2026-09-20

### Fixed
- 修复持久化数据位置不合规的问题：`comment_state.json`（已读位置状态）与 `video_context.json`（视频概括缓存）不再写入插件代码目录，统一迁移至 AstrBot 数据目录 `data/plugin_data/astrbot_plugin_bilibili_live_mod/` 下保存。

## [2.2.3] - 2026-09-20

### Added
- 新增视频评论区回复功能，现在机器人可以在视频评论区互动了。
- 新增cookie自动刷新功能。
- 新增登录双账号功能，现在可以使用小号互动来规避对大号的风控。

### Fixed
- 修复评论实际未成功发出时，日志没有相关报错的情况。

### Changed
- 修改插件名称为 `B站回复机器人` 。

## [1.0.0] - 2026-09-19

### Added
- 新增直播间回复功能，现在机器人可以在直播间互动了。

### Fixed
- 修复重复回复自身弹幕的问题

## [0.8.0] - 2026-09-19

### Changed
- 将原astrbot_plugin_bilibili_live 插件的blivedm版本更新到1.1.7

