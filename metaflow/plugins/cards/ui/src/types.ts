import type { VisualizationSpec } from "svelte-vega";

import type { EmbedOptions } from "vega-embed";

export type Route = [string, string];

export type Status = "success" | "error" | "idle" | "in-progress";

// used to build tree that supports pages and sections on the aside nav
export type PageHierarchy = Record<string, string[]>;

export type Boxes = Record<string, HTMLElement>;

/* ------------------------------- DATA TYPES ------------------------------- */

export interface Artifact {
  name: string | null;
  type: string;
  data: string;
  label?: string;
}

export type Artifacts = Artifact[];

export type TableDataCell =
  | boolean
  | string
  | number
  | ArtifactsComponent
  | DagComponent
  | HeadingComponent
  | ImageComponent
  | LogComponent
  | MarkdownComponent
  | ProgressBarComponent
  | TextComponent
  | VegaChartComponent;

export type TableColumns = string[];
export type TableData = TableDataCell[][];

// flowname/runid/stepname/taskid
export type PathSpecObject = {
  flowname: string;
  runid: string;
  stepname?: string;
  taskid?: string;
};

/* ----------------------------------- DAG ---------------------------------- */

export type Dag = Record<string, DagStep>;

export type StepType =
  | "linear"
  | "foreach"
  | "split-and"
  | "join"
  | "start"
  | "end";

export interface DagStep {
  box_ends: string | null;
  box_next: boolean;
  doc?: string;
  next: string[];
  type: StepType;
  created_at?: string;
  duration?: number;
  num_possible_tasks?: number;
  successful_tasks?: number;
  failed?: boolean;
  num_failed?: number;
}

/* -------------------------------- RESPONSE -------------------------------- */

export interface CardResponse {
  metadata: CardResponseMetaData;
  components: CardComponent[];
}

export interface CardResponseMetaData {
  stderr: string;
  stdout: string;
  created_at: string;
  finished_at: string;
  pathspec: string;
  version: number;
  template?: string;
}

/* ------------------------------- COMPONENTS ------------------------------- */

export interface SectionComponent {
  type: "section";
  contents?: CardComponent[];
  title?: string;
  id?: string;
  subtitle?: string;
  columns?: number;
}

export interface PageComponent {
  type: "page";
  id?: string;
  title: string; // used as an ID and label
  contents?: CardComponent[];
}

export interface ImageComponent {
  type: "image";
  src: string;
  id?: string;
  label?: string;
  description?: string;
}

export interface TitleComponent {
  type: "title";
  id?: string;
  text: string;
}

export interface SubtitleComponent {
  type: "subtitle";
  id?: string;
  text: string;
}

export interface TextComponent {
  type: "text";
  id?: string;
  text: string;
}
export interface ProgressBarComponent {
  type: "progressBar";
  id?: string;
  label?: string;
  max: number;
  value: number;
  unit?: string;
  details?: string;
}

export interface HeadingComponent {
  type: "heading";
  id?: string;
  title?: string;
  subtitle?: string;
}

// this component should support any tabular data, we will have to write a small
// transform to support pandas/numpy etc.
export interface TableComponent {
  type: "table";
  data: TableData;
  id?: string;
  columns: TableColumns;
  vertical?: boolean;
}

export interface ArtifactsComponent {
  type: "artifacts";
  id?: string;
  data: Artifacts;
}

export interface DagComponent {
  type: "dag";
  id?: string;
  data: Dag;
}

// handle stderr stdout strings
export interface LogComponent {
  type: "log";
  id?: string;
  data: string;
}

export interface MarkdownComponent {
  type: "markdown";
  id?: string;
  source: string;
}

export interface VegaChartComponent {
  type: "vegaChart";
  id?: string;
  spec: VisualizationSpec;
  data: Record<string, unknown>;
  options?: EmbedOptions;
}
// wrap all component options into a Component type
export type CardComponent =
  | ArtifactsComponent
  | DagComponent
  | HeadingComponent
  | ImageComponent
  | LogComponent
  | MarkdownComponent
  | PageComponent
  | ProgressBarComponent
  | SectionComponent
  | SubtitleComponent
  | TableComponent
  | TextComponent
  | TitleComponent
  | VegaChartComponent;
