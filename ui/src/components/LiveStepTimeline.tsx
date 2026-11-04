import React from 'react';
import { CheckCircle2, Clock, RotateCw } from 'lucide-react';
import { JobStep } from '../types';

interface LiveStepTimelineProps {
  steps: JobStep[];
  currentStatus: string;
}

const ORDERED_NODES = [
  { name: 'parse_alert', title: '1. Parse Alert', diff: 'low', model: 'gpt-4o-mini' },
  { name: 'fetch_context', title: '2. Fetch Context (MCP)', diff: 'mcp', model: 'prometheus+runbook' },
  { name: 'diagnose_root_cause', title: '3. Diagnose Root Cause', diff: 'high', model: 'gpt-4o' },
  { name: 'open_ticket', title: '4. Open Ticket (Idempotent)', diff: 'low', model: 'ticket_mcp' },
];

export const LiveStepTimeline: React.FC<LiveStepTimelineProps> = ({ steps, currentStatus }) => {
  return (
    <div className="bg-slate-900 border border-slate-800 rounded-xl p-5 shadow-2xl space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <h3 className="text-base font-bold text-white flex items-center gap-2">
            Live Step Timeline & Fenced State Execution
          </h3>
          <p className="text-xs text-slate-400">
            Postgres FencedCheckpointer prevents stale/zombie worker overwrite.
          </p>
        </div>
        <div className="flex items-center gap-2 text-xs">
          <span className="px-2.5 py-1 rounded-md bg-slate-800 border border-slate-700 text-slate-300">
            Status: <span className="font-semibold text-white capitalize">{currentStatus}</span>
          </span>
        </div>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-4 gap-4 pt-2">
        {ORDERED_NODES.map((node, index) => {
          const executed = steps.find((s) => s.step_name === node.name);
          const isDone = !!executed;
          const isRunning =
            !isDone &&
            (currentStatus === 'leased' || currentStatus === 'running') &&
            (index === 0 || !!steps.find((s) => s.step_name === ORDERED_NODES[index - 1].name));

          return (
            <div
              key={node.name}
              className={`relative rounded-xl p-4 border transition-all duration-300 flex flex-col justify-between h-44 ${
                isDone
                  ? 'bg-emerald-950/20 border-emerald-500/40 text-emerald-200 shadow-lg shadow-emerald-950/20'
                  : isRunning
                  ? 'bg-cyan-950/30 border-cyan-400 text-cyan-200 animate-pulse shadow-lg shadow-cyan-950/30'
                  : 'bg-slate-950/40 border-slate-800 text-slate-500'
              }`}
            >
              <div>
                <div className="flex items-center justify-between mb-2">
                  <span className="text-xs font-mono uppercase tracking-wider text-slate-400">
                    Step {index + 1}
                  </span>
                  {isDone ? (
                    <span className="flex items-center gap-1 text-xs text-emerald-400 font-medium">
                      <CheckCircle2 className="h-4 w-4" /> Done
                    </span>
                  ) : isRunning ? (
                    <span className="flex items-center gap-1 text-xs text-cyan-400 font-medium">
                      <RotateCw className="h-3.5 w-3.5 animate-spin" /> In Progress
                    </span>
                  ) : (
                    <span className="flex items-center gap-1 text-xs text-slate-500">
                      <Clock className="h-3.5 w-3.5" /> Pending
                    </span>
                  )}
                </div>

                <h4 className="font-semibold text-sm text-white mb-1">{node.title}</h4>
                <p className="text-xs text-slate-400 mb-2">Node: {node.name}</p>
              </div>

              <div className="pt-2 border-t border-slate-800/80 mt-auto">
                {isDone ? (
                  <div className="space-y-1 text-xs">
                    <div className="flex justify-between items-center text-slate-300">
                      <span>Worker / Attempt:</span>
                      <span className="font-mono font-bold text-amber-300 bg-amber-950/50 px-1.5 py-0.5 rounded border border-amber-500/30">
                        Attempt #{executed.attempt_id}
                      </span>
                    </div>
                    <div className="flex justify-between text-slate-400">
                      <span>Latency:</span>
                      <span>{executed.latency_ms} ms</span>
                    </div>
                  </div>
                ) : (
                  <div className="text-xs text-slate-500">
                    Target model: <span className="font-mono text-slate-400">{node.model}</span>
                  </div>
                )}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
};
