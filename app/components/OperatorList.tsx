'use client';
import { useState, useMemo, useRef, useEffect } from 'react';
import { categories } from '@/app/lib/operators';

interface OperatorListProps {
  selectedOp: string;
  setSelectedOp: (operatorName: string) => void;
}

const OperatorList = ({ selectedOp, setSelectedOp }: OperatorListProps) => {
  const [search, setSearch] = useState('');
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key === 'k') {
        e.preventDefault();
        inputRef.current?.focus();
      }
    };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, []);

  const filteredCategories = useMemo(() => {
    const query = search.toLowerCase().trim();
    if (!query) return Object.entries(categories);

    return Object.entries(categories)
                 .map(([categoryName, operators]) => {
                   const filtered = Object.fromEntries(
                     Object.entries(operators).filter(([opName]) =>
                       opName.toLowerCase().includes(query)
                     )
                   );
                   return [categoryName, filtered] as const;
                 })
                 .filter(([, operators]) => Object.keys(operators).length > 0);
  }, [search]);

  const totalMatches = filteredCategories.reduce((sum, [, ops]) => sum + Object.keys(ops).length, 0);

  return (
    <nav className="w-64 border-r border-zinc-200 dark:border-zinc-800 flex flex-col overflow-hidden">
      <div className="p-4 pb-2 flex-shrink-0">
        <h1 className="text-xl font-bold mb-3 text-zinc-900 dark:text-zinc-100">RxJS Marbles</h1>
        <div className="relative">
          <svg className="absolute left-2.5 top-1/2 -translate-y-1/2 w-4 h-4 text-zinc-400 pointer-events-none"
               fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth={ 2 }>
            <path strokeLinecap="round" strokeLinejoin="round"
                  d="M21 21l-4.35-4.35M11 19a8 8 0 100-16 8 8 0 000 16z"/>
          </svg>
          <input ref={ inputRef }
                 type="text"
                 placeholder="Filter operators…"
                 value={ search }
                 onChange={ (e) => setSearch(e.target.value) }
                 className="w-full pl-8 pr-8 py-1.5 text-sm rounded-md border border-zinc-300 dark:border-zinc-700
                       bg-white dark:bg-zinc-900 text-zinc-900 dark:text-zinc-100
                       placeholder-zinc-400 dark:placeholder-zinc-500
                       focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"/>
          { search && (
            <button onClick={ () => setSearch('') }
                    className="absolute right-2 top-1/2 -translate-y-1/2 text-zinc-400 hover:text-zinc-600
                         dark:hover:text-zinc-300 text-xs font-bold leading-none"
                    aria-label="Clear search">
              ✕
            </button>
          ) }
        </div>
        { search && (
          <p className="text-xs text-zinc-400 mt-1.5">
            { totalMatches } { totalMatches === 1 ? 'match' : 'matches' }
          </p>
        ) }
      </div>

      <div className="flex-1 overflow-y-auto p-4 pt-2">
        { filteredCategories.length === 0 ? (
          <p className="text-sm text-zinc-400 dark:text-zinc-500 text-center mt-8">
            No operators found
          </p>
        ) : (
            filteredCategories.map(([categoryName, operators]) => (
              <div key={ categoryName } className="mb-4">
                <h2 className="text-xs font-semibold uppercase tracking-wider text-zinc-500 mb-2">{ categoryName }</h2>
                <ul className="space-y-1">
                  { Object.keys(operators).map((opName) => (
                    <li key={ opName }>
                      <button onClick={ () => setSelectedOp(opName) }
                              className={ `w-full text-left px-3 py-1.5 rounded text-sm transition-colors ${
                                selectedOp === opName ? 'bg-blue-100 text-blue-900 dark:bg-blue-900 dark:text-blue-100 font-medium'
                                                      : 'text-zinc-700 dark:text-zinc-300 hover:bg-zinc-100 dark:hover:bg-zinc-800'
                              }` }>
                        { opName }
                      </button>
                    </li>
                  )) }
                </ul>
              </div>
            ))
          ) }
      </div>
    </nav>
  );
};
export default OperatorList;
