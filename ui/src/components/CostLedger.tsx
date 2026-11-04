import React from 'react';
import { Check } from 'lucide-react';
import { CostLedgerEntry } from '../types';

interface CostLedgerProps {
  ledger: CostLedgerEntry[];
  spentUsd: number;
  budgetUsd: number;
}

export const CostLedger: React.FC<CostLedgerProps> = ({ ledger, spentUsd, budgetUsd }) => {
  const budgetRatio = budgetUsd > 0 ? (spentUsd / budgetUsd) * 100 : 0;
  const isBudgetExceeded = spentUsd >= budgetUsd && budgetUsd > 0;

  return (
    <div className="bg-slate-900 border border-slate-800 rounded-xl p-5 shadow-2xl space-y-4">
      <div className="flex flex-wrap items-center justify-between gap-4">
        <div>
          <h3 className="text-base font-bold text-white flex items-center gap-2">
            Cost & Gateway Ledger
          </h3>
          <p className="text-xs text-slate-400">
            Per-step semantic routing, token tracking, and budget enforcement.
          </p>
        </div>

        {/* Budget Bar */}
        <div className="bg-slate-950 border border-slate-800 px-4 py-2 rounded-lg flex items-center gap-4">
          <div className="text-xs">
            <span className="text-slate-400">Spent / Budget: </span>
            <span
              className={`font-mono font-bold ${
                isBudgetExceeded ? 'text-rose-400' : 'text-emerald-400'
              }`}
            >
              ${spentUsd.toFixed(5)}
            </span>
            <span className="text-slate-500 font-mono"> / ${budgetUsd.toFixed(2)}</span>
          </div>
          <div className="w-24 bg-slate-800 h-2 rounded-full overflow-hidden">
            <div
              className={`h-full ${isBudgetExceeded ? 'bg-rose-500' : 'bg-emerald-500'}`}
              style={{ width: `${Math.min(budgetRatio, 100)}%` }}
            />
          </div>
        </div>
      </div>

      <div className="overflow-x-auto">
        <table className="w-full text-left text-xs">
          <thead>
            <tr className="border-b border-slate-800 text-slate-400 font-mono uppercase tracking-wider">
              <th className="py-2.5 px-3">Step Node</th>
              <th className="py-2.5 px-3">Attempt</th>
              <th className="py-2.5 px-3">Routed Model</th>
              <th className="py-2.5 px-3">Tokens (In/Out)</th>
              <th className="py-2.5 px-3">Cache Badge</th>
              <th className="py-2.5 px-3">Latency</th>
              <th className="py-2.5 px-3 text-right">Cost (USD)</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-800/60 font-sans">
            {ledger.length === 0 ? (
              <tr>
                <td colSpan={7} className="text-center py-8 text-slate-500">
                  No step executions recorded yet. Trigger an alert above to watch live.
                </td>
              </tr>
            ) : (
              ledger.map((entry, idx) => (
                <tr key={entry.id || idx} className="hover:bg-slate-800/30 transition-colors">
                  <td className="py-2.5 px-3 font-semibold text-white">{entry.step_name}</td>
                  <td className="py-2.5 px-3">
                    <span className="font-mono px-2 py-0.5 rounded bg-amber-950/40 text-amber-300 border border-amber-600/30">
                      #{entry.attempt_id}
                    </span>
                  </td>
                  <td className="py-2.5 px-3">
                    <span
                      className={`font-mono px-2 py-0.5 rounded border text-[11px] ${
                        entry.model.includes('mini')
                          ? 'bg-blue-950/40 text-blue-300 border-blue-600/30'
                          : entry.model.includes('gpt-4o')
                          ? 'bg-purple-950/40 text-purple-300 border-purple-600/30'
                          : 'bg-slate-800 text-slate-300 border-slate-700'
                      }`}
                    >
                      {entry.model}
                    </span>
                  </td>
                  <td className="py-2.5 px-3 font-mono text-slate-300">
                    {entry.tokens_in} / {entry.tokens_out}
                  </td>
                  <td className="py-2.5 px-3">
                    {entry.cached ? (
                      <span className="inline-flex items-center gap-1 font-mono px-2 py-0.5 rounded bg-emerald-950/40 text-emerald-300 border border-emerald-600/30">
                        <Check className="h-3 w-3" /> HIT
                      </span>
                    ) : (
                      <span className="font-mono px-2 py-0.5 rounded bg-slate-800 text-slate-400">
                        MISS
                      </span>
                    )}
                  </td>
                  <td className="py-2.5 px-3 font-mono text-slate-400">{entry.latency_ms} ms</td>
                  <td className="py-2.5 px-3 text-right font-mono font-medium text-emerald-400">
                    ${Number(entry.cost_usd).toFixed(6)}
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
};
