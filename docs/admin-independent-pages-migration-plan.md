# 管理后台独立页迁移清单

目标：后台只保留独立页，不再保留承载完整功能的管理弹层。  
统一结构：`admin-hub.html` 作为唯一总入口，各后台能力以独立 URL 承载，弹窗仅保留极少量浏览器级确认。

## 统一原则

1. 管理后台只有一个总入口
- 统一从 [admin-hub.html](/Users/joshua/Downloads/project/AskBible/admin-hub.html) 进入。

2. 每个功能只有一个真实归属页
- 不再同时维护“首页弹层版”和“独立页版”两套后台。

3. 重功能一律做独立页
- 包括查询、编辑、预览、批量操作、发布、同步、权限、统计等。

4. 旧管理弹层只保留过渡跳转
- 过渡期可继续兼容 `/#openAdmin` 深链。
- 但不再新增功能，不再继续扩面板。

5. 所有后台子页统一壳结构
- 左侧导航
- 顶部标题
- 一句说明
- 操作栏
- 筛选区
- 主工作区
- 状态区 / 帮助区

## 当前状态总览

### A. 已经是独立页

这些页已经符合“独立页优先”的方向，应继续保留并统一样式，不再回退到弹层：

- `admin-hub.html`
- `bible-character-designer.html`
- `chapter-key-people.html`
- `illustration-admin.html`
- `chapter-illustration-library.html`
- `video-center.html`
- `generated-png-thumbs.html`
- `site-chrome.html`
- `seo-settings.html`
- `color-themes.html`
- `promo-edit.html`
- `home-layout-map.html`
- `admin-analytics.html`

### B. 仍在旧管理弹层中的核心功能

这些功能现在主要由 [main.js](/Users/joshua/Downloads/project/AskBible/main.js) 动态插入 `.modal-card-admin` 中，属于优先迁出对象：

- 规则编辑 `ruleEditor`
- 测试生成 `testGenerate`
- 已发布内容 `published`
- 圣经版本 `scripture_versions`
- 内容版本 `content_versions_menu`
- 部署与同步 `deploy`
- 积分体系 `points_system`
- 贡献审核 `question_review`
- 权限管理 `admin_users`

## 迁移优先级

### 第一批：最该先迁

这几项工作量大、停留时间长、信息密度高，最不适合继续留在弹层里：

1. 已发布内容
- 建议新页：`/admin-published.html`
- 理由：有查询、统计、整本发布、章节详情、JSON 编辑、历史记录，明显是完整工作台。

2. 部署与同步
- 建议新页：`/admin-deploy.html`
- 理由：包含打包、上传、应用升级、回滚、远端同步、备份恢复、审计、系统密钥，复杂度最高。

3. 权限管理
- 建议新页：`/admin-users.html`
- 理由：涉及管理员分级、列表刷新、初始化口令、权限安全，应该单独管理。

4. 贡献审核
- 建议新页：`/admin-question-review.html`
- 理由：当前本质上已经是一个嵌入审核工作区，独立页会更清楚。

### 第二批：内容运营核心页

这些功能跟内容生成链路强相关，也适合尽快从弹层迁出：

5. 规则编辑
- 建议新页：`/admin-rule-editor.html`
- 理由：这是内容生产中枢，应该直接成为正式子页。

6. 测试生成
- 建议新页：`/admin-test-generate.html`
- 理由：需要长时间对比结果、修改参数、查看输出，独立页体验更稳。

7. 圣经版本
- 建议新页：`/admin-scripture-versions.html`
- 理由：有列表、编辑、保存、删除，已经是标准 CRUD 页。

8. 内容版本
- 建议新页：`/admin-content-versions.html`
- 理由：本质是版本配置页，适合做成轻量独立页。

### 第三批：最后收尾

9. 积分体系
- 建议新页：`/admin-points-system.html`
- 理由：虽然功能相对集中，但仍然是完整配置页面，最终也应迁出。

## 推荐后台信息架构

后台首页建议长期固定为 4 组：

- 内容生产
- 人物与插画
- 站点与展示
- 系统与数据

其中旧弹层里迁出的功能建议这样归组：

### 内容生产
- 规则编辑
- 测试生成
- 已发布内容
- 圣经版本
- 内容版本

### 系统与数据
- 部署与同步
- 权限管理
- 贡献审核
- 积分体系

## 每个新后台页的统一模板

每个独立页建议统一成下面这套骨架：

1. 顶部区
- 页面标题
- 一句用途说明
- 返回管理后台首页按钮

2. 操作区
- 主操作按钮
- 次操作按钮
- 当前状态提示

3. 筛选区
- 版本 / 语言 / 书卷 / 用户 / 时间等筛选

4. 主内容区
- 列表、表单、编辑器、预览区

5. 侧边信息区
- 说明文案
- 风险提示
- 最近操作记录

## 最省事的迁移顺序

建议按下面顺序实施，风险最低：

1. 先创建新独立页壳
- 只把旧弹层功能原样搬过去，先不大改逻辑。

2. 让 `admin-hub.html` 先指向新页
- 入口统一后，再慢慢清理旧弹层。

3. 保留 `/#openAdmin:*` 兼容跳转一段时间
- 但跳过去后尽量引导到新页。

4. 等新页稳定后，删除旧弹层对应面板
- 从 [main.js](/Users/joshua/Downloads/project/AskBible/main.js) 里移除 `ensure...TabExists()` 和配套初始化逻辑。

## 实施时的判断标准

一个功能如果满足下面任一条，就不该继续放在弹层里：

- 停留超过 3 分钟
- 需要多步操作
- 需要搜索 / 筛选 / 预览 / 批量处理
- 需要复制链接或回到上一步
- 需要并排查看多个信息块

## 结论

最优方案不是重做一切，而是：

- 保留现有独立页成果
- 把旧管理弹层剩余功能逐页迁出
- 让 `admin-hub.html` 成为唯一后台总入口
- 最终把旧弹层降级成兼容层，直至删除
