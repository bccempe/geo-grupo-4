import L from 'leaflet';

const SYMBOLS_EMOJI = {
  hospital: { emoji: '🏥', label: 'Centro de Salud' },
  proposed: { emoji: '📌', label: 'Centro Propuesto' },
  origin: { emoji: '📍', label: 'Origen' },
  north: { emoji: '⬆️', label: 'Norte' },
}

const renderMapIcon = (emoji, label) => L.divIcon({
  className: 'emoji-marker',
  html: `<span class="emoji-marker-glyph" role="img" aria-label="${label}">${emoji}</span>`,
  iconSize: [28, 28],
  iconAnchor: [14, 24],
  popupAnchor: [0, -20]
});

const renderMapIcon_FromSymbol = (symbol) => {
  if (!symbol || !symbol.emoji || !symbol.label) {
    console.warn(`Invalid symbol object: ${JSON.stringify(symbol)}`);
    return null;
  }
  return renderMapIcon(symbol.emoji, symbol.label);
}

export { SYMBOLS_EMOJI, renderMapIcon_FromSymbol };