'use client';
import { useRef, useCallback, type MouseEvent } from 'react';
import { type Timeline } from '@/app/lib/useMarbles';

const SVG_WIDTH = 700;
const SVG_HEIGHT = 60;
const PADDING = 30;
const TRACK_Y = SVG_HEIGHT / 2;
const MARBLE_RADIUS = 18;
const ARROW_SIZE = 8;

// Convert marble t (0–100) → pixel x
const tToX = (t: number) => PADDING + (t / 100) * (SVG_WIDTH - 2 * PADDING);

// Convert pixel x → marble t (0–100) — the inverse
const xToT = (x: number) => ((x - PADDING) / (SVG_WIDTH - 2 * PADDING)) * 100;

interface MarbleDiagramProps {
  timeline: Timeline;
  color?: string;
  label?: string;
  onMoveMarble?: (marbleId: string, newT: number) => void;
  onMoveError?: (newT: number) => void;
}

const MARBLE_PALETTE = [
  '#ef4444', // red
  '#f97316', // orange
  '#eab308', // yellow
  '#22c55e', // green
  '#06b6d4', // cyan
  '#3b82f6', // blue
  '#8b5cf6', // violet
  '#ec4899', // pink
  '#14b8a6', // teal
  '#f43f5e', // rose
];

const COLORS = {
  blue  : '#3b82f6',
  green : '#22c55e',
  orange: '#f97316',
  purple: '#a855f7',
};

type ColorName = keyof typeof COLORS;

export default function MarbleDiagram({
                                        timeline,
                                        color = 'blue',
                                        label,
                                        onMoveMarble,
                                        onMoveError,
                                      }: MarbleDiagramProps) {
  const useMultiColor = color === 'multi';
  const solidColor = COLORS[color as ColorName] ?? COLORS.blue;
  const getMarbleColor = (index: number) =>
    useMultiColor ? MARBLE_PALETTE[index % MARBLE_PALETTE.length] : solidColor;
  const isDraggable = !!onMoveMarble;

  const svgRef = useRef<SVGSVGElement>(null);
  const dragRef = useRef<string | null>(null); // marble ID being dragged

  const clientXToT = useCallback((clientX: number): number => {
    const svg = svgRef.current;
    if (!svg) return 0;

    // Create an SVG point and transform from screen → SVG coordinate space
    const pt = svg.createSVGPoint();
    pt.x = clientX;
    pt.y = 0;
    const svgPt = pt.matrixTransform(svg.getScreenCTM()?.inverse());
    return Math.max(0, Math.min(100, xToT(svgPt.x)));
  }, []);

  const handleMouseDown = useCallback(
    (e: MouseEvent, marbleId: string) => {
      if (!isDraggable) return;
      e.preventDefault();
      dragRef.current = marbleId;

      const handleMouseMove = (moveEvent: globalThis.MouseEvent) => {
        if (!dragRef.current || !onMoveMarble) return;
        const newT = clientXToT(moveEvent.clientX);
        onMoveMarble(dragRef.current, newT);
      };

      const handleMouseUp = () => {
        dragRef.current = null;
        window.removeEventListener('mousemove', handleMouseMove);
        window.removeEventListener('mouseup', handleMouseUp);
      };

      // Attach to window so dragging works even if mouse leaves the SVG
      window.addEventListener('mousemove', handleMouseMove);
      window.addEventListener('mouseup', handleMouseUp);
    },
    [isDraggable, onMoveMarble, clientXToT],
  );

  const handleErrorMouseDown = useCallback(
    (e: MouseEvent) => {
      if (!onMoveError) return;
      e.preventDefault();

      const handleMouseMove = (moveEvent: globalThis.MouseEvent) => {
        onMoveError(clientXToT(moveEvent.clientX));
      };

      const handleMouseUp = () => {
        window.removeEventListener('mousemove', handleMouseMove);
        window.removeEventListener('mouseup', handleMouseUp);
      };

      window.addEventListener('mousemove', handleMouseMove);
      window.addEventListener('mouseup', handleMouseUp);
    },
    [onMoveError, clientXToT],
  );

  return (
    <div className="mb-6">
      { label && (
        <h3 className="text-sm font-semibold text-zinc-500 mb-1">{ label }</h3>
      ) }

      <svg ref={ svgRef }
           viewBox={ `0 0 ${ SVG_WIDTH } ${ SVG_HEIGHT }` }
           className="w-full max-w-3xl"
           style={ { overflow: 'visible' } }>
        <line x1={ PADDING } y1={ TRACK_Y } x2={ SVG_WIDTH - PADDING } y2={ TRACK_Y } stroke="#a1a1aa" strokeWidth={ 2 }/>
        <polygon points={ `
            ${ SVG_WIDTH - PADDING },${ TRACK_Y }
            ${ SVG_WIDTH - PADDING - ARROW_SIZE },${ TRACK_Y - ARROW_SIZE / 2 }
            ${ SVG_WIDTH - PADDING - ARROW_SIZE },${ TRACK_Y + ARROW_SIZE / 2 }
          ` }
                 fill="#a1a1aa"/>
        { timeline.completion != null && (
          <line x1={ tToX(timeline.completion) }
                y1={ TRACK_Y - 20 }
                x2={ tToX(timeline.completion) }
                y2={ TRACK_Y + 20 }
                stroke="#a1a1aa"
                strokeWidth={ 3 }/>
        ) }

        { timeline.error != null && (
          <g onMouseDown={ onMoveError ? handleErrorMouseDown : undefined }
             style={ { cursor: onMoveError ? 'grab' : 'default' } }>
            {/* Invisible hit area — easier to grab than thin lines */}
            <circle cx={ tToX(timeline.error.t) }
                    cy={ TRACK_Y }
                    r={ 14 }
                    fill="transparent"/>
            <line x1={ tToX(timeline.error.t) - 10 }
                  y1={ TRACK_Y - 10 }
                  x2={ tToX(timeline.error.t) + 10 }
                  y2={ TRACK_Y + 10 }
                  stroke="#ef4444"
                  strokeWidth={ 3 }/>
            <line x1={ tToX(timeline.error.t) + 10 }
                  y1={ TRACK_Y - 10 }
                  x2={ tToX(timeline.error.t) - 10 }
                  y2={ TRACK_Y + 10 }
                  stroke="#ef4444"
                  strokeWidth={ 3 }/>
          </g>
        ) }

        { timeline.marbles.map((marble, index) => {
          const cx = tToX(marble.t);
          return (
            <g key={ marble.id }
               onMouseDown={ isDraggable ? (e) => handleMouseDown(e, marble.id) : undefined }
               style={ { cursor: isDraggable ? 'grab' : 'default' } }>
              {/* Circle */ }
              <circle cx={ cx }
                      cy={ TRACK_Y }
                      r={ MARBLE_RADIUS }
                      fill={ getMarbleColor(index) }
                      stroke="white"
                      strokeWidth={ 2 }/>
              {/* Value label centered in the circle */ }
              <text x={ cx }
                    y={ TRACK_Y }
                    textAnchor="middle"
                    dominantBaseline="central"
                    fill="white"
                    fontSize={ 12 }
                    fontWeight="bold"
                    fontFamily="monospace"
                    style={ { pointerEvents: 'none', userSelect: 'none' } }>
                { marble.c }
              </text>
            </g>
          );
        }) }
      </svg>
    </div>
  );
};
