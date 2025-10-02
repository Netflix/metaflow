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
  | ValueBoxComponent
  | VegaChartComponent
  | PythonCodeComponent
  | EventsTimelineComponent;

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

// TODO: add support for switch-split
export type StepType =
  | "linear"
  | "foreach"
  | "split-and"
  | "switch"
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
  condition?: string;
  switch_cases?: Record<string, string>;
  pathToStep?: string;
  connections?: string[];
}

export type DagStructure = {
  [key: string]: {
    stepName: string;
    pathToStep: string;
    connections: string[];
    node: HTMLElement;
  };
};

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

export interface PythonCodeComponent {
  type: "pythonCode";
  id?: string;
  data: string;
}

export interface ValueBoxComponent {
  type: "valueBox";
  id?: string;
  title?: string;
  value: string | number;
  subtitle?: string;
  theme?: string; // CSS class for styling
  change_indicator?: string; // e.g., "Up 30% VS PREVIOUS 30 DAYS"
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

export interface EventsTimelineComponent {
  type: "eventsTimeline";
  id?: string;
  title?: string;
  events: EventsTimelineEvent[];
  config: {
    show_stats: boolean;
    show_relative_time: boolean;
    max_events: number;
  };
  stats?: EventsTimelineStats;
}

export interface EventsTimelineEvent {
  metadata: Record<string, any>;
  payloads: Record<string, CardComponent>;
  event_id: string;
  received_at: number;
  style_theme?: string;
  priority?: string;
}

export interface EventsTimelineStats {
  total_events: number;
  displayed_events: number;
  last_update?: number;
  first_event?: number;
  events_per_minute?: number;
  total_runtime_seconds?: number;
  finished: boolean;
}

export interface JSONViewerComponent {
  type: "jsonViewer";
  id?: string;
  json_string: string;
  collapsible: boolean;
  show_copy_button: boolean;
  max_height?: string;
  title: string;
}

export interface YAMLViewerComponent {
  type: "yamlViewer";
  id?: string;
  yaml_string: string;
  collapsible: boolean;
  show_copy_button: boolean;
  max_height?: string;
  title: string;
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
  | ValueBoxComponent
  | VegaChartComponent
  | PythonCodeComponent
  | EventsTimelineComponent
  | JSONViewerComponent
  | YAMLViewerComponent;
