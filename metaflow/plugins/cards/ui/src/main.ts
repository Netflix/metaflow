// load app
import App from "./App.svelte";

let app;

// wrapping in try/catch to let user know if its missing #app
try {
  app = new App({
    target: document.getElementById("app") as Element,
    props: {},
  });
} catch (err: any) {
  throw new Error(err);
}

export default app;
