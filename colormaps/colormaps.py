from matplotlib.colors import ListedColormap, LinearSegmentedColormap
from matplotlib import cm
import numpy


def get_colormap(name):
    if name == 'BlYlBn':
        bottom = cm.get_cmap('YlOrBr_r', 50)
        top = cm.get_cmap('Blues', 50)
        newcolors = numpy.vstack((
            bottom(numpy.linspace(0, 0.2, 2)),
            bottom(numpy.linspace(0.2, 0.4, 3)),
            bottom(numpy.linspace(0.4, 0.6, 5)),
            bottom(numpy.linspace(0.6, 0.8, 10)),
            bottom(numpy.linspace(0.8, 1, 30)),
            top(numpy.linspace(0, 0.25, 30)),
            top(numpy.linspace(0.25, 0.45, 10)),
            top(numpy.linspace(0.45, 0.65, 5)),
            top(numpy.linspace(0.65, 0.85, 3)),
            top(numpy.linspace(0.85, 1, 2)),
        ))
        newcmp = ListedColormap(newcolors, name='CDI')
        return cm.get_cmap(newcmp)
    if name == 'DroughtOutlook':
        colours = [(255/255, 255/255, 255/255),
                   (17/255, 130/255, 0/255),
                   (190/255, 225/255, 140/255),
                   (232/255, 201/255, 60/255),
                   (100/255, 70/255, 45/255),
                   (235/255, 120/255, 45/255)]
        return LinearSegmentedColormap.from_list('DroughtOutlook', colours)
