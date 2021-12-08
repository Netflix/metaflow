// we are importing the library locally because its custom compiled.
// eventually we should be able to rely on tree-shaking
import "./prism";
import "./prism.css";

// load app
import App from "./App.svelte";

let app;

// wrapping in try/catch to let user know if its missing #app
try {
  app = new App({
    target: document.getElementById("app"),
    props: {},
  });
} catch (err) {
  throw new Error(err);
}

export default app;
