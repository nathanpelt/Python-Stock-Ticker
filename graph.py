import sys
from PyQt4.QtGui import *  
import pyqtgraph as pg

# creates PyQt4 Application object
app = QApplication(sys.argv)
# creates window object
window = QWidget()
# sets window title to "Stock Datawarehouse"
window.setWindowTitle("Stock Datawarehouse") 
# sets changes default window icon to my icon
window.setWindowIcon(QIcon("datawarehouse.ico"))

# creates some window objects
back_button = QPushButton('back')
plot = pg.PlotWidget()

# Creates a grid layout
layout = QGridLayout()
window.setLayout(layout)
# add widgets to the layout 
layout.addWidget(back_button, 0, 0)   
layout.addWidget(plot, 0, 1, 3, 1)

# Displays window
window.show()
# Needed so the gui window stays open until user closes it
app.exec_()
