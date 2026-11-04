export interface JobStep {
  id: string;
  job_id: string;
  attempt_id: number;
  step_name: string;
  model: string;
  tokens_in: number;
  tokens_out: number;
  cost_usd: number;
  latency_ms: number;
  created_at?: string;
  status?: 'pending' | 'running' | 'completed' | 'failed';
}

export interface CostLedgerEntry {
  id: string;
  job_id: string;
  attempt_id: number;
  step_name: string;
  model: string;
  tokens_in: number;
  tokens_out: number;
  cost_usd: number;
  latency_ms: number;
  cached?: boolean;
  recorded_at?: string;
}

export interface JobState {
  id: string;
  tenant_id: string;
  status: string;
  attempt_id: number;
  budget_usd: number;
  spent_usd: number;
  payload?: any;
  steps: JobStep[];
  ledger: CostLedgerEntry[];
}

