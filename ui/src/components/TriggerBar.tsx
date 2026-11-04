import React from 'react';
import { Play, Flame, RefreshCw, Cpu } from 'lucide-react';

interface TriggerBarProps {
  onTriggerAlert: () => void;
  onSimulateCrash: () => void;
  isLoading: boolean;
  activeJobId: string | null;
  activeAttempt: number;
}

export const TriggerBar: React.FC<TriggerBarProps> = ({
  onTriggerAlert,
  onSimulateCrash,
  isLoading,
  activeJobId,
  activeAttempt,
}) => {
  return (
    <div className="bg-slate-900 border border-slate-800 rounded-xl p-5 shadow-2xl flex flex-wrap items-center justify-between gap-4">
      <div className="flex items-center gap-3">
        <div className="h-10 w-10 rounded-lg bg-cyan-500/10 border border-cyan-500/30 flex items-center justify-center text-cyan-400">
          <Cpu className="h-5 w-5" />
        </div>
        <div>
          <h2 className="text-lg font-bold text-white flex items-center gap-2">
            Control Tower
            {activeJobId && (
              <span className="text-xs font-mono font-normal px-2 py-0.5 rounded-full bg-slate-800 text-slate-300 border border-slate-700">
                Job: {activeJobId.slice(0, 8)}...
              </span>
            )}
          </h2>
          <p className="text-xs text-slate-400">
            Active Attempt Token:{' '}
            <span className="font-semibold text-cyan-300">
              Worker #{activeAttempt > 0 ? activeAttempt : 1} (attempt_id: {activeAttempt})
            </span>
          </p>
        </div>
      </div>

      <div className="flex items-center gap-3">
        <button
          onClick={onTriggerAlert}
          disabled={isLoading}
          className="flex items-center gap-2 px-4 py-2.5 rounded-lg font-medium text-sm transition-all duration-150 bg-cyan-600 hover:bg-cyan-500 text-white shadow-lg shadow-cyan-950 disabled:opacity-50 disabled:cursor-not-allowed cursor-pointer"
        >
          {isLoading ? (
            <RefreshCw className="h-4 w-4 animate-spin" />
          ) : (
            <Play className="h-4 w-4 fill-white" />
          )}
          Send Sample Alert
        </button>

        <button
          onClick={onSimulateCrash}
          disabled={!activeJobId}
          className="flex items-center gap-2 px-4 py-2.5 rounded-lg font-medium text-sm transition-all duration-150 bg-rose-600/90 hover:bg-rose-500 text-white border border-rose-500 shadow-lg shadow-rose-950/50 disabled:opacity-40 disabled:cursor-not-allowed cursor-pointer"
        >
          <Flame className="h-4 w-4" />
          Simulate Worker Crash (SIGKILL)
        </button>
      </div>
    </div>
  );
};
