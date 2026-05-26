'use client';
import { useState } from 'react';
import { useMarbles } from './lib/useMarbles';
import OperatorList from './components/OperatorList';
import MarbleDiagram from './components/MarbleDiagram';


const Home = () => {
  const [selectedOp, setSelectedOp] = useState<string>('map');
  const { inputTimelines, outputTimeline, label, moveMarble, moveError } = useMarbles(selectedOp);


  return (
    <div className="flex h-screen overflow-hidden bg-zinc-50 dark:bg-zinc-950">

      {/* Nav Menu*/ }
      <OperatorList selectedOp={ selectedOp } setSelectedOp={ setSelectedOp }/>

      {/*  Main Content */ }
      <main className="flex-1 p-8 overflow-y-auto">
        <h2 className="text-2xl font-mono font-bold text-zinc-900 dark:text-zinc-100 mb-6">{ label }</h2>

        {/*  Input timelines */ }
        { inputTimelines.map((timeline, i) => (
          <MarbleDiagram key={ i }
                         timeline={ timeline }
                         label={ `Input ${ i + 1 }` }
                         color="multi"
                         onMoveMarble={(marbleId, newT) => moveMarble(i, marbleId, newT)}
                         onMoveError={(newT) => moveError(i, newT)}/>
        )) }

        {/* Divider */ }
        <div className="border-t-2 border-dashed border-zinc-300 dark:border-zinc-700 my-6 relative">
          <span className="absolute -top-3 left-4 bg-zinc-50 dark:bg-zinc-950 px-2 text-sm text-zinc-500">{ label }</span>
        </div>

        {/* Output timeline */ }
        <MarbleDiagram timeline={outputTimeline}
                       label="Output"
                       color="green"/>
      </main>
    </div>
  );
};
export default Home;
