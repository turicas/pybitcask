# coding: utf-8

import json

import numpy as np
import matplotlib
matplotlib.use('agg')
import matplotlib.pyplot as plt


def barplot(xlabels, curve_data, filename):
    colors = list('brbr')

    xlabels = map(str, xlabels)
    curves_y = [ydata for name, ydata in curve_data]
    curves_name = [name for name, ydata in curve_data]
    N = len(curves_y[0])
    ind = np.arange(N)
    width = 1.0 / (N + 1)

    fig, ax = plt.subplots()
    rects = [ax.bar(ind + width * i, ydata, width, color=colors.pop())
             for i, ydata in enumerate(curves_y)]

    ax.set_xticks(ind + width)
    ax.set_xticklabels(xlabels)
    legends = [rect[0] for rect in rects]
    ax.legend(legends, curves_name, loc=2)

    def autolabel(r):
        for rect in r:
            height = rect.get_height()
            ax.text(rect.get_x() + rect.get_width()/2., 1.05*height,
                    '%d' % int(height),
                    ha='center', va='bottom')
    plt.grid(True)
    map(autolabel, rects)
    plt.show()
    plt.savefig(filename)


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('input_filename')
    parser.add_argument('output_filename')
    args = parser.parse_args()

    with open(args.input_filename) as fobj:
        data = json.load(fobj)

    barplot(data['xlabels'], data['curve_data'], args.output_filename)


if __name__ == '__main__':
    main()
