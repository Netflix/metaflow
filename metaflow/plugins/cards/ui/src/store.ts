import type * as types from "./types";
import { writable } from "svelte/store";
import type { Writable } from "svelte/store";

export const cardData: Writable<types.CardResponse | undefined> =
  writable(undefined);

// Fetch the data from the window, or fallback to the example data file
export const setCardData: (cardDataId: string) => void = (cardDataId) => {
  try {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-argument
    const data = JSON.parse((window as any).__MF_DATA__[cardDataId]) as types.CardResponse;
    cardData.set(data);
  } catch (error) {
    // for now we are loading an example card if there is no string
    fetch("/card-example.json")
      .then((resp) => resp.json())
      .then((data: types.CardResponse) => {
        cardData.set(data);
      })
      .catch(console.error);
  }

};

export const modal: Writable<types.CardComponent | undefined> =
  writable(undefined);
