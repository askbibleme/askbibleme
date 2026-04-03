# AskBible Mobile Release Checklist

## 1) Prerequisites
- Install Xcode (iOS) and Android Studio (Android)
- Install CocoaPods: `sudo gem install cocoapods`
- Confirm Capacitor platforms exist: `ios/` and `android/`

## 2) Build Sync
- Android 一键：更新启动器图标（来自 `assets/icons/source-app-icon.png`）并同步工程  
  `npm run android:prep`
- 或分步：`npm run android:icons` → `npm run cap:sync` → `npm run cap:android`（打开 Android Studio）
- Open iOS project: `npx cap open ios`

## 3) App Identity
- `capacitor.config.ts`
  - `appId`: `me.askbible.app`
  - `appName`: `AskBible`
  - `server.url`: `https://askbible.me`
- Android package should match `appId`
- iOS bundle identifier should match `appId`

## 4) App Icons and Launch
- 安全区：自适应前景使用 `drawable/ic_launcher_foreground_inset.xml`（四边 21dp，对齐 66dp 关键区）；网站/PWA 与旧版 mipmap 用脚本按 **66/108** 比例缩图后垫色 `#4A443F`。
- 重新生成：`bash scripts/refresh-app-icons.sh`（网站）与 `bash scripts/generate-android-launcher-icons.sh`（Android），或 `npm run android:icons`。
- iOS icon source:
  - Replace assets in `ios/App/App/Assets.xcassets/AppIcon.appiconset`
- Keep launch screen simple text/logo in both platforms

## 5) Permission and Privacy
- Current app only needs network access
- If adding camera/files/push in future:
  - Add Android permissions in `AndroidManifest.xml`
  - Add iOS usage strings in `ios/App/App/Info.plist`

## 6) QA Smoke Test
- Launch app on Android emulator/device
- Launch app on iPhone simulator/device
- Confirm app loads `https://askbible.me`
- Confirm login, chapter switching, and admin entry work
- Confirm PWA/service worker does not block content refresh

## 7) 网站直链下载（客户自选 APK）

- 在 Android Studio 构建 **release APK**（非 AAB 亦可用于直链）。
- 仓库根目录执行：`npm run apk:stage`（复制到 `downloads/askbible-release.apk` 并更新 `downloads/version.json`）。
- 部署时把 **`downloads/askbible-release.apk`** 与 **`downloads/version.json`** 放到站点根目录（与 `index.html` 同级）；用户打开 **`/download.html`** 下载。
- APK 与密钥勿提交 Git（`downloads/.gitignore` 已忽略 `*.apk`）。

## 8) Google Play（Android）上架流程

### 8.1 签名密钥（本机一次）
- 在 `android/` 下生成密钥（密码自行保管，勿提交仓库）：
  `keytool -genkey -v -keystore askbible-release.keystore -alias askbible -keyalg RSA -keysize 2048 -validity 10000`
- 复制 `android/keystore.properties.example` 为 `android/keystore.properties`，填四项，其中 `storeFile=askbible-release.keystore`（文件与 `keystore.properties` 同目录）。

### 8.2 打 AAB
- `npm run cap:sync`（或至少 `npx cap sync android`）
- Android Studio：**Build → Generate Signed Bundle / APK → Android App Bundle**，选 release 密钥；或 **Build → Build Bundle(s) / APK(s) → Build Bundle(s)**（已配置 `keystore.properties` 时 release 会自动签名）。
- 产物：`android/app/build/outputs/bundle/release/app-release.aab`
- 每次上架前在 `android/app/build.gradle` 的 `defaultConfig` 里 **递增 `versionCode`**，并更新 `versionName`。

### 8.3 Play Console
- **Create app** → 填写默认语言、应用名、类型、免费/付费。
- **完成** 商店详情：短说明、完整说明、图标（512）、手机截图、功能图（如需要）。
- **政策**：隐私政策 URL（有登录/账号一般必填）、内容分级、目标受众、数据安全表单。
- **版本**：**Testing → Internal testing** → 创建版本 → 上传 `app-release.aab` → 添加测试员邮箱 → 发测试链接。
- 内测无问题后，再走 **Production** 或 **Closed testing** 逐步放开。

### 8.4 账号侧
- 顶栏若提示 **finish setting up developer account** 或 **Android developer verification**，需先完成，否则可能无法正式发布。
- iOS:
  - Set Team and Signing in Xcode
  - Archive and upload to TestFlight

## 9) Update Strategy
- Web content updates: deploy website only
- Native shell updates (icons/permissions/plugins): submit new store build
