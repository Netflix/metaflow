import type * as types from "./types";
import { writable } from "svelte/store";
import type { Writable } from "svelte/store";

export const cardData = (() => {
  const store: Writable<types.CardResponse | undefined> = writable(undefined);

  try {
    const data = JSON.parse((window as any).__DATA__);
    store.set(data);
  } catch (error) {
    // for now we are loading an example card if there is no string
    fetch("/card-example.json")
      .then((resp) => resp.json())
      .then((data) => {
        store.set(data);
      });
  }

  return store;
})();

export const modal: Writable<types.CardComponent | undefined> =
  writable(undefined);
