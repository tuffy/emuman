# Emulation Manager

This is a ROM manager primarily for MAME that's based
around storing ROM files in non-merged, uncompressed sets.
That means instead of storing all the ROM files for `mrdo`
in a `mrdo.zip` file, we will create a `mrdo` directory
and place all its ROM files in that directory without the use of
zip files at all.  One directory per machine, and one file per ROM.

## The Advantages

Storing files this way makes them incredibly easy to audit and manage.
If a new version of MAME adds more ROMs to a machine, simply
move them into the proper directory.  If ROMs get renamed,
simply rename them.  No compressed archives to mess around with.

### Wouldn't This Waste a Lot of Space?

I use a compressed filesystem (like ZFS) to ensure my ROM files
don't use more space than they would if they were stuffed into zip files.
But even if I didn't, ROMs don't use a lot of space to begin with
compared to MAME's CHD files.

### What About BIOS Files?

Although having the same `uni-bios_3_3.rom` in 50 different places
wouldn't take up a lot of space in absolute terms, it's still
not very efficient.  Therefore, this manager uses hard links
so that the same file may be in many directories, while
being stored on disk only once.

# Getting Started

Installation is a simple matter of:

    cargo install emuman

which will install the main emuman program.

# MAME

The MAME subcommand is for handling arcade hardware
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

## Adding New ROMs for Machines

Given a source directory of raw unzipped ROMs and a target
directory, you can add all the ROMS for a given machine using

    emuman mame add -i inputdir -o outputdir machine

Specifying multiple input directories is okay,
as is specifying multiple machines to add.
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
The report will be send to standard output for easy filtering,
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
Machines can be sorted by description, year or manufacturer
using the `--sort` flag, with description used by default.

Machines highlighted in magenta are those MAME considers
to be partially working.  Machines highlighted in red are those
that MAME considers to be preliminary and probably don't work
at all.

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
Software can be sorted by description, year or publisher
using the `--sort` flag, with description used by default.

As with MAME, software highlighted in magenta are titles MAME considers
to be partially working.  Software highlighted in red are titles
that MAME considers to be not working.

## Splitting ROMs

Sometimes ROMs from other sources comes in a combined state,
which is at odds with MAME's "one file per ROM" policy.
The split option divides a ROM into its component parts, if possible.

    emuman mess split -o outputdir list rom

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
