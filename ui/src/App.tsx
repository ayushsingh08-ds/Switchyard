import React, { useState, useEffect } from 'react';
import { Activity, Terminal } from 'lucide-react';
import { TriggerBar } from './components/TriggerBar';
import { LiveStepTimeline } from './components/LiveStepTimeline';
import { CostLedger } from './components/CostLedger';
import { JobState } from './types';

export const App: React.FC = () => {
  const [jobState, setJobState] = useState<JobState | null>(null);
  const [isLoading, setIsLoading] = useState<boolean>(false);
  const [eventSource, setEventSource] = useState<EventSource | null>(null);
  const [logs, setLogs] = useState<string[]>([]);

  const addLog = (msg: string) => {
    setLogs((prev) => [
      `[${new Date().toLocaleTimeString()}] ${msg}`,
      ...prev.slice(0, 19),
    ]);
  };

  const handleTriggerAlert = async () => {
    setIsLoading(true);
    addLog('Triggering sample alert job to FastAPI ingestion endpoint...');
    if (eventSource) {
      eventSource.close();
      setEventSource(null);
    }

    try {
      const samplePayload = {
        alert_name: 'HighErrorRate',
        service: 'payment-service',
        severity: 'critical',
        timestamp: new Date().toISOString(),
      };

      const res = await fetch('/api/v1/jobs', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          traceparent: `00-${Array.from({ length: 32 }, () => Math.floor(Math.random() * 16).toString(16)).join('')}-0000000000000001-01`,
        },
        body: JSON.stringify({
          tenant_id: 'tenant-demo',
          payload: samplePayload,
          budget_usd: 1.0,
        }),
      });

      if (!res.ok) {
        throw new Error(`Failed to enqueue job: ${res.statusText}`);
      }

      const data = await res.json();
      addLog(`Job created: ${data.id} (status: ${data.status})`);

      setJobState({
        id: data.id,
        tenant_id: data.tenant_id,
        status: data.status,
        attempt_id: data.attempt_id || 1,
        budget_usd: data.budget_usd || 1.0,
        spent_usd: 0,
        payload: samplePayload,
        steps: [],
        ledger: [],
      });

      // Subscribe to SSE stream
      subscribeToJobEvents(data.id);
    } catch (err: any) {
      addLog(`Error triggering job: ${err.message}`);
    } finally {
      setIsLoading(false);
    }
  };

  const subscribeToJobEvents = (jobId: string) => {
    addLog(`Opening Server-Sent Events stream for job ${jobId}...`);
    const es = new EventSource(`/api/v1/jobs/${jobId}/events`);

    es.onopen = () => {
      addLog('SSE connection established.');
    };

    es.onmessage = (event) => {
      try {
        const payload = JSON.parse(event.data);
        if (payload.type === 'snapshot' || payload.type === 'update') {
          setJobState((prev) => {
            if (!prev) return payload.job;
            return {
              ...prev,
              ...payload.job,
              steps: payload.job.steps || prev.steps,
              ledger: payload.job.ledger || prev.ledger,
            };
          });
        } else if (payload.type === 'step') {
          addLog(`Step completed: ${payload.step.step_name} on attempt #${payload.step.attempt_id}`);
          setJobState((prev) => {
            if (!prev) return null;
            const updatedSteps = [...prev.steps.filter((s) => s.step_name !== payload.step.step_name), payload.step];
            return {
              ...prev,
              steps: updatedSteps,
              attempt_id: payload.step.attempt_id,
            };
          });
        } else if (payload.type === 'ledger') {
          setJobState((prev) => {
            if (!prev) return null;
            return {
              ...prev,
              ledger: [...prev.ledger, payload.entry],
              spent_usd: prev.spent_usd + (payload.entry.cost_usd || 0),
            };
          });
        } else if (payload.type === 'status') {
          addLog(`Status changed to: ${payload.status}`);
          setJobState((prev) => (prev ? { ...prev, status: payload.status } : null));
          if (payload.status === 'completed' || payload.status === 'failed') {
            addLog(`Job execution reached terminal state: ${payload.status}`);
            es.close();
          }
        }
      } catch (err: any) {
        console.error('Error parsing SSE event:', err);
      }
    };

    es.onerror = (err) => {
      console.warn('SSE stream error, retrying/closing:', err);
    };

    setEventSource(es);
  };

  const handleSimulateCrash = async () => {
    if (!jobState) return;
    addLog(`Sending SIGKILL to simulate worker crash on attempt #${jobState.attempt_id}...`);
    try {
      const res = await fetch('/api/v1/demo/kill-worker', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          job_id: jobState.id,
          attempt_id: jobState.attempt_id,
        }),
      });
      const data = await res.json();
      addLog(`Worker termination triggered: ${data.detail || 'Killed'}. Reaper will lease to Worker #${jobState.attempt_id + 1}!`);
    } catch (err: any) {
      addLog(`Crash simulation request failed: ${err.message}`);
    }
  };

  useEffect(() => {
    return () => {
      if (eventSource) {
        eventSource.close();
      }
    };
  }, [eventSource]);

  return (
    <div className="min-h-screen bg-slate-950 text-slate-100 flex flex-col font-sans">
      {/* Top Header */}
      <header className="border-b border-slate-800 bg-slate-900/60 backdrop-blur sticky top-0 z-50 px-6 py-4 flex items-center justify-between">
        <div className="flex items-center gap-3">
          <div className="h-9 w-9 rounded-lg bg-cyan-500/10 border border-cyan-500/30 flex items-center justify-center text-cyan-400">
            <Activity className="h-5 w-5" />
          </div>
          <div>
            <h1 className="font-extrabold text-base tracking-tight text-white flex items-center gap-2">
              SWITCHYARD
              <span className="text-[10px] font-mono uppercase px-2 py-0.5 rounded bg-cyan-950/60 text-cyan-400 border border-cyan-500/30">
                Resilience Engine
              </span>
            </h1>
            <p className="text-xs text-slate-400">
              Deterministic WAL Broker + Fenced LangGraph Checkpointer + Cost Gateway
            </p>
          </div>
        </div>
      </header>

      {/* Main Content Area */}
      <main className="flex-1 max-w-7xl w-full mx-auto p-6 space-y-6">
        {/* Trigger Bar */}
        <TriggerBar
          onTriggerAlert={handleTriggerAlert}
          onSimulateCrash={handleSimulateCrash}
          isLoading={isLoading}
          activeJobId={jobState?.id || null}
          activeAttempt={jobState?.attempt_id || 1}
        />

        {/* Live Step Timeline */}
        <LiveStepTimeline
          steps={jobState?.steps || []}
          currentStatus={jobState?.status || 'idle'}
        />

        {/* Cost & Gateway Ledger */}
        <CostLedger
          ledger={jobState?.ledger || []}
          spentUsd={jobState?.spent_usd || 0}
          budgetUsd={jobState?.budget_usd || 1.0}
        />

        {/* Live Event Stream / Terminal Console */}
        <div className="bg-slate-900 border border-slate-800 rounded-xl p-5 shadow-2xl space-y-2">
          <div className="flex items-center justify-between text-xs font-mono text-slate-400 border-b border-slate-800 pb-2">
            <span className="flex items-center gap-1.5 text-slate-300">
              <Terminal className="h-4 w-4 text-cyan-400" /> Live Event Telemetry Stream
            </span>
            <span>Buffered logs: {logs.length}</span>
          </div>
          <div className="bg-slate-950 rounded-lg p-3 font-mono text-xs text-slate-300 space-y-1 max-h-36 overflow-y-auto">
            {logs.length === 0 ? (
              <span className="text-slate-600">Awaiting events...</span>
            ) : (
              logs.map((log, index) => (
                <div key={index} className="leading-relaxed">
                  <span className="text-cyan-400">{log.split(' ')[0]}</span>{' '}
                  <span>{log.slice(log.indexOf(' ') + 1)}</span>
                </div>
              ))
            )}
          </div>
        </div>
      </main>
    </div>
  );
};

export default App;
