import sys
from vnpy.event import EventEngine
from vnpy.trader.engine import MainEngine

from vnpy_ctabacktester.ui import BacktesterManager
from vnpy_ctabacktester import CtaBacktesterApp
from vnpy.trader.ui import QtWidgets

def main():
    """Start VeighNa Trader"""
    app = QtWidgets.QApplication([])

    event_engine = EventEngine()
    main_engine = MainEngine(event_engine)
    main_engine.add_app(CtaBacktesterApp)
    backMg = BacktesterManager(main_engine, event_engine)
    backMg.show()

    sys.exit(app.exec()) 

if __name__ == "__main__":
    main()
