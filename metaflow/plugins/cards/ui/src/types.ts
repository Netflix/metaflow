import type { ChartConfiguration } from "chart.js";

export type Route = [string, string];

export type Status = "success" | "error" | "idle" | "in-progress";

// used to build tree that supports pages and sections on the aside nav
export type PageHierarchy = Record<string, string[]>;

export type Boxes = Record<string, HTMLElement>;

/* ------------------------------- DATA TYPES ------------------------------- */

export interface Artifact {
  name: string|null;
  type: string;
  data: string;
  label?: string;
}

export type Artifacts = Artifact[];

export type TableDataCell = boolean | string | number | ArtifactsComponent | BarChartComponent | DagComponent | HeadingComponent | LineChartComponent | LogComponent | MarkdownComponent | TextComponent;

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
  subtitle?: string;
  columns?: number;
}

export interface PageComponent {
  type: "page";
  title: string; // used as an ID and label
  contents?: CardComponent[];
}

export interface ImageComponent {
  type: "image";
  src: string;
  label?: string;
  description?: string;
}

export interface TitleComponent {
  type: "title";
  text: string;
}

export interface SubtitleComponent {
  type: "subtitle";
  text: string;
}

export interface TextComponent {
  type: "text";
  text: string;
}

export interface HeadingComponent {
  type: "heading";
  title?: string;
  subtitle?: string;
}

// this component should support any tabular data, we will have to write a small
// transform to support pandas/numpy etc.
export interface TableComponent {
  type: "table";
  data: TableData;
  columns: TableColumns;
  vertical?: boolean;
}

// any chart that uses charts.js can be configured entirely custom by
// passing in the custom chart config object.
export interface DefaultChart {
  config?: ChartConfiguration;
}

// you can pass in only a single line of data/label and we will render the chart
export interface LineChartComponent extends DefaultChart {
  type: "lineChart";
  data?: number[];
  labels?: string[] | number[];
}

// you can pass in only a single line of data/label and we will render the chart
export interface BarChartComponent extends DefaultChart {
  type: "barChart";
  data?: number[];
  labels?: string[] | number[];
}

export interface ArtifactsComponent {
  type: "artifacts";
  data: Artifacts;
}

export interface DagComponent {
  type: "dag";
  data: Dag;
}

// handle stderr stdout strings
export interface LogComponent {
  type: "log";
  data: string;
}

export interface MarkdownComponent {
  type: "markdown";
  source: string;
}

// wrap all component options into a Component type
export type CardComponent =
  | ArtifactsComponent
  | BarChartComponent
  | DagComponent
  | HeadingComponent
  | ImageComponent
  | LineChartComponent
  | LogComponent
  | MarkdownComponent
  | PageComponent
  | SectionComponent
  | SubtitleComponent
  | TableComponent
  | TextComponent
  | TitleComponent;
