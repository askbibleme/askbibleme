# AskBible Mobile Release Checklist

## 1) Prerequisites
- Install Xcode (iOS) and Android Studio (Android)
- Install CocoaPods: `sudo gem install cocoapods`
- Confirm Capacitor platforms exist: `ios/` and `android/`

## 2) Build Sync
- Run `npx cap sync`
- Open Android project: `npx cap open android`
- Open iOS project: `npx cap open ios`

## 3) App Identity
- `capacitor.config.ts`
  - `appId`: `me.askbible.app`
  - `appName`: `AskBible`
  - `server.url`: `https://askbible.me`
- Android package should match `appId`
- iOS bundle identifier should match `appId`

## 4) App Icons and Launch
- Android icon source:
  - Replace adaptive icon layers in `android/app/src/main/res/mipmap-*`
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

## 7) Store Preparation
- Android:
  - Build signed AAB in Android Studio
  - Upload to Google Play internal testing
- iOS:
  - Set Team and Signing in Xcode
  - Archive and upload to TestFlight

## 8) Update Strategy
- Web content updates: deploy website only
- Native shell updates (icons/permissions/plugins): submit new store build
