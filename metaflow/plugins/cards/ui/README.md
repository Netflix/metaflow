# @cards UI

This directory contains the files that generate the Javascript and CSS used in the standalone HTML file when the cards are generated.

The code is written in [svelte](https://svelte.dev/).

## To run locally

- `yarn install`
- `yarn dev`

This will run a [server](http://localhost:8080) showing a single card, using example data from `public/card-example.json`.

## To make changes to be used by metaflow

- `yarn install`
- Make your changes to the `.svelte` and/or `.css` files
- `yarn lint` to ensure the types are correct
- `yarn build`

This will put a `main.js` and a `bundle.css` file in a directory that will be picked up by metaflow when it is running a flow. The output directory is specified in `package.json`.

To run a flow:

- `python MYCARD.py run --with card`
- `python dummy.py card view <RUN_NUMBER>/<STEP_NAME>/<TASK_NUMBER>`

You can get `<RUN_NUMBER>/<STEP_NAME>/<TASK_NUMBER>` from the output from the first step that runs metaflow.
