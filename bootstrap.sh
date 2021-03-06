#! /bin/sh
echo Running autotools...

if test ! -d "../oobase"; then
	echo You need to link oobase to ../oobase, try ln -s
fi

aclocal -I m4 && \
autoheader && \
libtoolize --force --no-warn && \
automake --foreign --add-missing && \
autoconf
