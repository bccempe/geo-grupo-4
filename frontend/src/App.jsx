import React, { useState } from 'react';
import Navbar from './components/Navbar';
import IsochroneTab from './tabs/IsochroneTab';
import HealthDesertTab from './tabs/HealthDesertTab';
import CoverageTab from './tabs/CoverageTab';

export default function App() {
  const [activeTab, setActiveTab] = useState('isochrone');

  return (
    <div className="min-h-screen bg-slate-950 text-slate-100 flex flex-col font-['Inter',sans-serif]">
      {/* Header Navbar */}
      <Navbar activeTab={activeTab} setActiveTab={setActiveTab} />

      {/* Main Content Area */}
      <main className="flex-1 max-w-7xl w-full mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {activeTab === 'isochrone' && <IsochroneTab />}
        {activeTab === 'desert' && <HealthDesertTab />}
        {activeTab === 'coverage' && <CoverageTab />}
      </main>

      {/* Footer */}
      <footer className="border-t border-slate-900 bg-slate-950 py-6 text-center text-xs text-slate-500">
        <p>GeoSalud RM &copy; {new Date().getFullYear()} - Sistema de Información Geográfica y Accesibilidad de Salud</p>
      </footer>
    </div>
  );
}
