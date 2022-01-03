import type * as types from "./types";
import { writable } from "svelte/store";
import type { Writable } from "svelte/store";

export const cardData: Writable<types.CardResponse | undefined> = (() => {
  const store: Writable<types.CardResponse | undefined> = writable(undefined);

  try {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
    const data = JSON.parse((window as any).__DATA__) as types.CardResponse;
    store.set(data);
  } catch (error) {
    // for now we are loading an example card if there is no string
    fetch("/card-example.json")
      .then((resp) => resp.json())
      .then((data: types.CardResponse) => {
        store.set(data);
      })
      .catch(console.error);
  }

  return store;
})();

export const modal: Writable<types.CardComponent | undefined> =
  writable(undefined);
