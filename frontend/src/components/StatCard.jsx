import React from 'react';

export default function StatCard({ title, value, subtitle, icon: Icon, color = 'sky' }) {
  const colorStyles = {
    sky: 'from-sky-500/10 to-blue-500/5 border-sky-500/30 text-sky-400',
    emerald: 'from-emerald-500/10 to-teal-500/5 border-emerald-500/30 text-emerald-400',
    rose: 'from-rose-500/10 to-pink-500/5 border-rose-500/30 text-rose-400',
    amber: 'from-amber-500/10 to-orange-500/5 border-amber-500/30 text-amber-400',
    indigo: 'from-indigo-500/10 to-purple-500/5 border-indigo-500/30 text-indigo-400',
  }[color] || 'from-slate-800 to-slate-900 border-slate-700 text-slate-300';

  return (
    <div className={`relative overflow-hidden rounded-2xl p-5 border bg-gradient-to-br backdrop-blur-md shadow-xl ${colorStyles}`}>
      <div className="flex items-center justify-between">
        <div>
          <p className="text-xs uppercase tracking-wider font-semibold text-slate-400">{title}</p>
          <h3 className="text-2xl font-bold text-white mt-1.5 font-['Outfit'] tracking-tight">
            {value}
          </h3>
          {subtitle && <p className="text-xs text-slate-400 mt-1">{subtitle}</p>}
        </div>
        {Icon && (
          <div className="p-3 rounded-xl bg-slate-900/60 border border-slate-700/50 shadow-inner">
            <Icon className="w-6 h-6" />
          </div>
        )}
      </div>
    </div>
  );
}
