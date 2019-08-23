# Emulation Manager

This is a ROM manager primarily for MAME that's based
around storing ROM files in non-merged, uncompressed sets.
That means instead of storing all the ROM files for `mrdo`
in a `mrdo.zip` file, we will create a `mrdo` directory
and place all its ROM files in that directory, like:

    mrdo/
      a4-01.bin
      c4-02.bin
      e4-03.bin
      f10--1.bin
      f4-04.bin
      h5-05.bin
      j10--4.bin
      j2-u001.bin
      k5-06.bin
      n8-07.bin
      r8-08.bin
      s8-09.bin
      t02--3.bin
      u02--2.bin
      u8-10.bin

One directory per machine, and one file per ROM.

MAME is extremly lenient about how its ROM files are stored,
and will accept this layout just as easily as it will accept
a directory full of `.zip` files.

## The Advantages

Storing files this way makes them incredibly easy to audit and manage.
If a new version of MAME adds more ROMs to a machine, simply
move them into the proper directory.  If ROMs get renamed,
simply rename them.  No compressed archives to mess around with.

## Wouldn't This Waste a Lot of Space?

I use a compressed filesystem (like ZFS) to ensure my ROM files
don't use more space than they would if they were stuffed into zip files.
But even if I didn't, ROMs don't use a lot of space to begin with
compared to MAME's CHD files.

## What About Shared Files?

Even though ROMs don't use a lot of space, storing multiple
copies of the same files is still needlessly wasteful.
This manager uses hard links by default, so identical files
used by different machines will be shared via hard links
and stored on disk only once.  Using `mrdo` and `mrdofix` as
an example:

    mrdo/             mrdofix/
      a4-01.bin
      c4-02.bin
      e4-03.bin
                        d1
                        d10
                        d2
                        d9
                        dofix.d3
                        dofix.d4
      f10--1.bin  <=>   f10--1.bin
      f4-04.bin
      h5-05.bin   <=>   h5-05.bin
      j10--4.bin  <=>   j10--4.bin
      j2-u001.bin <=>   j2-u001.bin
      k5-06.bin   <=>   k5-06.bin
      n8-07.bin   <=>   n8-07.bin
      r8-08.bin   <=>   r8-08.bin
      s8-09.bin
      t02--3.bin  <=>   t02--3.bin
      u02--2.bin  <=>   u02--2.bin
      u8-10.bin

Ten of these ROM files are identical between the two versions,
so we'll simply hard-link them together (indicated by the `<=>`).

# Getting Started

Installation is a simple matter of:

    cargo install emuman

which will install the main emuman program.
This program has several subcommands (git-style) to perform
actions on different ROM dumps.
Use the built-in help commands for a quick rundown, if necessary.

# MAME

The MAME subcommand is for managing arcade hardware
as well as ROMs needed for home consoles, portables as so on.
Handling the software for non-arcade hardware
is done using the `mess` commands, illustrated in the next section.

## Populating the Database

`emuman` needs to be seeded with MAME driver information
so that it knows which machines require which ROMs.

One way to get this information is on the
[MAME downloads page](https://www.mamedev.org/release.html).
It's the `mameXXXXlx.zip` link labeled
"full driver information in XML format".
Be sure to unzip the file once it's been downloaded.
Then you can seed it like:

    emuman mame create mameXXXX.xml

However, another way is to get the driver information
directly from MAME itself using its `-listxml` option, like:

    mame -listxml | emuman mame create

In both cases, the cached files will be stored
in an appropriate system-specific directory.

## Generating a List of Machines

Simply using

    emuman mame list

will generate a list of all machines supported by MAME
as a table and sent to standard output.

Machines highlighted in yellow are those MAME considers
to be partially working.  Machines highlighted in red are those
that MAME considers to be preliminary and probably don't work
at all.

This list will be quite large, so you may add a search parameter
to restrict the list to things you want, like

    emuman mame list parameter

The search parameter looks for a subset of the description
(e.g. `emuman mame list "Mr. Do"` searches for all
games with `Mr. Do` in their descriptions), a subset of
the creator (e.g. `emuman mame list Atari` searches
for all games with `Atari` in their creator string),
the game's name prefix (e.g. `emuman mame list mrdo` searches
for all games whose name starts with `mrdo`), and the game's
release year (e.g. `emuman mame list 1982` searches for
all games released in 1982).

The same parameter is applied to all fields simultaneously
and all matching hits are returned.

In addition, the `--sort` parameter enables you to
sort output by description (the default), creator
or release year.

The `--simple` parameter cuts down a lot of extra information
from the description and creator fields which may not be useful.

## Adding New ROMs for Machines

Given a source directory of raw unzipped ROMs and a target
directory, you can add all the ROMS for a given machine using

    emuman mame add -i inputdir -o outputdir machine

Specifying multiple machines to add is okay.
If no directories are specified, the current working directory is used.
If no machines are specified, `emuman` tries to add
ROMs to as many machines as possible.

If the input directory and output directory are on the same
filesystem, `emuman` will add ROMs using hard links
rather than copying.  This is how it ensures reused ROMs
(like BIOS files) aren't duplicated multiple times in
the output directory.

As usual, `emuman` includes no ROM files and so you will
have to find those on your own.

### Unzipping Many ROMs at Once

If you already have a lot of zipped ROM files,
here's a simple Python script to extract them all at once,
each in their own directory:

    import sys
    from subprocess import call
    from os.path import splitext
    from os import unlink

    for z in sys.argv[1:]:
        (dirname, ext) = splitext(z)
        if ext == ".zip":
            call(["unzip", "-d", dirname, z])
            unlink(z)
        elif ext == ".7z":
            call(["7za", "x", "-o" + dirname, z])
            unlink(z)

As you can see, the zip archives will be removed once
the files have been extracted.

## Verifying ROM Sets

Given a directory with your added ROM sets, machines can be verified using

    emuman mame verify -d outputdir machine

If no directory is specified, the current working directory is used.
If no machines are specified, `emuman` tries to verify as
many machines as it finds in the root of the output directory.
The report will be sent to standard output for easy filtering,
but will *not* be generated in any particular order.

Machines will be reported as OK only if their directories
contain all the correct ROMs with the correct names and nothing else.
Missing files or incorrect files will be reported as BAD,
as will machine directories with extra files that need to be removed.

## Generating a Report

Given a directory with your added ROM sets, a simple report
can be generated with

    emuman mame report -d outputdir

This report will be formatted as a table and sent to standard output.
The report can be given a search term just as the `list` parameter,
described above.

Note that the report doesn't verify a machine's ROMs at all.

# MESS

Yes, MESS isn't really a "thing" anymore, having long since
been re-absorbed into MAME proper.  I'm simply using the
name because it's short.  These command are for handling
software for a machine, and most of them require a software
list as an argument.

A lot of the MESS functionality is quite similar to MAME's.

## Populating the Database

MAME's source code contains a `hash` directory
containing many XML files, one per software list.
Adding them is simple

    emuman mess create hash/*.xml

One can get a quick report of all supported software lists using

    emuman mess report

The software list names will be used in all the other `mess` options.

## Generating a List of Software

Simply using

    emuman mess list

will generate a report of all software lists.  Or, using

    emuman mess list some_list

will generate a report of all software for the given software list.
Since adding software requires knowing its name in MESS,
this is an easy way to find that name.

The list can be filtered the same way as the MAME list,
described above.

## Adding New ROMs for a Software List

Given a source directory of raw unzipped ROMs, a target directory,
and a software list, you can add all the ROMs for a given piece
of software using

    emuman mess add -i inputdir -o outputdir list software

Specifying multiple pieces of software to add is okay.
If no directories are specified, the current working directory is used.
If no software is specified, `emuman` tries to add
ROMs to as many pieces of software as possible.

## Verify Software for a Software List

Given a directory with your software, a software list can be verified using

    emuman mess verify -d outputdir list software

If no directory is specified, the current working directory is used.
If no software is specified, `emuman` tries to verify as
many pieces of software as it finds in the root of the output directory.
The report will be send to standard output for easy filtering,
but will *not* be generated in any particular order.

As with MAME, software will be reported as OK only if their directories
contain all the correct ROMs with the correct names and nothing else.
Missing files or incorrect files will be reported as BAD,
as will software directories with extra files that need to be removed.

## Generating a Report for a Software List

Given a directory with your added ROM sets and a software list,
a simple report can be generated with

    emuman mess report -d outputdir list

This report will be formatted as a table and sent to standard output.
It can also be sorted and filtered the same way as MAME's
software report, described above.

## Splitting ROMs

Sometimes ROMs from other sources comes in a combined state,
which is at odds with MAME's "one file per ROM" policy.
The split option divides a ROM into its component parts, if possible.

    emuman mess split -o outputdir list rom

Many ROMs can be recombined by simply concatenating them together
(with `cat`), with the notable exception of NES ROMs
which lose their 16 byte iNES header during the conversion.

# Redump

Though not MAME-specific, `emuman` also includes some helper
utilities for managing Redump-verified disc images.

## Populating the Database

After downloading the desired `.dat` files, populate the database with

    emuman redump create *.dat

## Verifying a Disc Image

All the tracks for a given CD image can be verified with

    emuman redump verify *.bin *.cue

The verification results will be displayed to standard output
for easy filtering.

## Splitting a Disc Image

Sometimes a disc image comes as a single `.bin` file
(MAME's `chdman` will extract to this format).
If you would like to turn this file into a set of
Redump-verified tracks, it can be split with

    emuman redump split file.bin

Multiple `.bin` files can be specified at once,
as can an output directory.
