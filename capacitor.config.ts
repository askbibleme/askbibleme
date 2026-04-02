import type { CapacitorConfig } from "@capacitor/cli";

const config: CapacitorConfig = {
  appId: "me.askbible.app",
  appName: "AskBible",
  webDir: "dist-capacitor",
  server: {
    url: "https://askbible.me",
    cleartext: false,
  },
};

export default config;
