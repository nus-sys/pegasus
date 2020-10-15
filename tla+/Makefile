SHELL=/bin/bash

.PHONY: all clean

pegasus.pdf: pegasus.ps
	ps2pdf $<

pegasus.ps pegasus.tex: pegasus.tla tlatex.sty tla.sty tla2tools.jar
	java -cp tla2tools.jar tla2sany.SANY $<
	java -cp tla2tools.jar tla2tex.TLA -shade -style tlatex.sty $<
	rm -f pegasus.{log,fls,aux,dvi,fdb_latexmk}

tlatex.sty: tla2tools.jar
	echo "Extracting tlatex.sty..."
	jar -xf tla2tools.jar tla2tex/tlatex.sty
	mv tla2tex/tlatex.sty $@
	rm -rf tla2tex
	echo "\\input{tla.sty}" >> $@

tla2tools.jar:
	wget "https://github.com/tlaplus/tlaplus/releases/latest/download/tla2tools.jar"

clean:
	rm -rf *.tex *.ps *.pdf tlatex.sty tla2tools.jar
