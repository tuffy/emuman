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

MAME is extremely lenient about how its ROM files are stored
and will accept a directory full of machine subdirectories
just as easily as it will accept a directory full of machine `.zip` files.

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

    mrdo/           mrdofix/
      a4-01.bin
      c4-02.bin
      e4-03.bin
                      d1
                      d10
                      d2
                      d9
                      dofix.d3
                      dofix.d4
      f10--1.bin  ⇔   f10--1.bin
      f4-04.bin
      h5-05.bin   ⇔   h5-05.bin
      j10--4.bin  ⇔   j10--4.bin
      j2-u001.bin ⇔   j2-u001.bin
      k5-06.bin   ⇔   k5-06.bin
      n8-07.bin   ⇔   n8-07.bin
      r8-08.bin   ⇔   r8-08.bin
      s8-09.bin
      t02--3.bin  ⇔   t02--3.bin
      u02--2.bin  ⇔   u02--2.bin
      u8-10.bin

Ten of these ROM files are identical between the two versions,
so we'll simply hard-link them together (indicated by the `⇔`).

## What About Hard Link Limits?

Even the most commonly shared ROMs are spread between
less than 2,000 different machines, which will comfortably
fit into the limit of most Linux filesystems - and those
most common ROMs are very tiny and for drivers that don't even work.
Shared ROMs for drivers that do work number less than 300 machines,
which will fit on nearly every filesystem that supports hard links.

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
Then you can seed it like:

    emuman mame create mameXXXX.zip

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

## Generating a List of Specific Machines

Using

    emuman mame games name1 name2 ...

will generate a list of the given games (by their short name),
in the given order, formatted as a table (as in `list`, above)
and sent to standard output.

This isn't particularly helpful on its own, but may
come in handy when combined with `ls` or scripts.

## Adding New ROMs for Machines

Given a source directory of zipped or unzipped ROMs and a target
directory, you can add all the ROMs for a given machine using

    emuman mame add -i inputdir -r outputdir machine

Specifying multiple machines to add is okay.
If no machines are specified, `emuman` will try to add as many ROMs
as possible from the input directory to the output directory.
If no directories are specified, the current working directory is used.

If the input directory and output directory are on the same
filesystem, `emuman` will add ROMs using hard links
rather than copying.  This is how it ensures reused ROMs
(like BIOS files) aren't duplicated multiple times in
the output directory.

As usual, `emuman` includes no ROM files and so you will
have to find those on your own.

## Verifying ROM Sets

Given a directory with your added ROM sets, machines can be verified using

    emuman mame verify -r outputdir machine

If no directory is specified, the current working directory is used.
If no machines are specified, `emuman` tries to verify as
many machines as it finds in the root of the output directory.
The report will be sent to standard output for easy filtering
and sorted by game name.

Machines will be reported as OK only if their directories
contain all the correct ROMs with the correct names and nothing else.
Missing files or incorrect files will be reported as BAD,
as will machine directories with extra files that need to be removed.

## Generating a Report

Given a directory with your added ROM sets, a simple report
can be generated with

    emuman mame report -r outputdir

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

    emuman mess create hash/a2600.xml hash/nes.xml ...

One can get a quick report of all supported software lists using

    emuman mess list

The software list names will be used in all the other `mess` options.

## Generating a List of Software

Simply using

    emuman mess list

will generate a report of all software lists.  Or, using

    emuman mess list softlist

will generate a report of all software for the given software list.
Since adding software requires knowing its name in MESS,
this is an easy way to find that name.

The list can be filtered the same way as the MAME list,
described above.

## Generating a List of Specific Games

Using

    emuman mess games softlist name1 name2 ...

will generate a list of the given games (by their short name)
in the given software list, in the given order,
formatted as a table (as in `list`, above)
and sent to standard output.

As with the MAME version this isn't particularly helpful on its own,
but may come in handy when combined with `ls` or scripts.

## Adding New ROMs for a Software List

Given a source directory of zipped or unzipped ROMs, a target directory,
and a software list, you can add all the ROMs for a given piece
of software using

    emuman mess add -i inputdir -r outputdir softlist software

Specifying multiple pieces of software to add is okay.
If no software is specified, `emuman` will try to add as many ROMs
as possible from the input directory to the output directory.
If no directories are specified, the current working directory is used.
If no software is specified, `emuman` tries to add
ROMs to as many pieces of software as possible.

## Verify Software for a Software List

Given a directory with your software, a software list can be verified using

    emuman mess verify -r outputdir softlist software

If no directory is specified, the current working directory is used.
If no software is specified, `emuman` tries to verify as
many pieces of software as it finds in the root of the output directory.
The report will be send to standard output for easy filtering
and sorted by game name.

As with MAME, software will be reported as OK only if their directories
contain all the correct ROMs with the correct names and nothing else.
Missing files or incorrect files will be reported as BAD,
as will software directories with extra files that need to be removed.

## Generating a Report for a Software List

Given a directory with your added ROM sets and a software list,
a simple report can be generated with

    emuman mess report -r outputdir softlist

This report will be formatted as a table and sent to standard output.
It can also be sorted and filtered the same way as MAME's
software report, described above.

## Splitting ROMs

Sometimes ROMs from other sources comes in a combined state,
which is at odds with MAME's "one file per ROM" policy.
The split option divides a ROM into its component parts, if possible.

    emuman mess split -r outputdir rom

Because combined ROMs are identfied by size and hash,
specifying a software list is unnecessary.

To reverse the process, many ROMs can be recombined by
simply concatenating them together (with `cat`),
with the notable exception of NES ROMs which lose their 16 byte
iNES header during the conversion.

# Redump

Though not MAME-specific, `emuman` also includes some helper
utilities for managing Redump-verified disc images.
These utilities work very much like the ones for MESS.

## Populating the Database

After downloading the desired `.dat` files from the Redump website
(which are normally stored in individual `.zip` files),
populate the database with

    emuman redump create *.zip

The `dat` files will be given names to be used in subsequent options.

## Generating a List of Software

Use

    emuman redump list

to generate a report of all software lists.
All the known software for a given list can be queried with:

    emuman redump list softlist

Note that unlike MESS, Redump software list names contain
spaces (like "Sega - Saturn") and will need to be quoted
appropriately in a shell.

And, as in MESS, software lists can be filtered with a search term.

However, because Redump has a less information about its rips,
trying to filter by creator or year won't work.
This is also why Redump has no `report` option;
there simply isn't anything else to report that a simple
directory listing wouldn't provide.

## Adding New Tracks for Software

Given a source directory containing raw `.cue` and `.bin` files,
all the tracks for a given piece of software can be added
using

    emuman redump add -i inputdir -r outputdir softlist software

Specifying multiple pieces of software to add is okay.
If no software is specified, `emuman` will try to add as many tracks
as possible from the input directory to the output directory.

## Verifying a Disc Tracks

All the tracks for a given software list can be verified with

    emuman redump verify softlist -r outputdir software

If no directory is specified, the current working directory is used.
If no software is specified, `emuman` tries to verify as
many pieces of software as it finds in the root of the output directory.
The report will be send to standard output for easy filtering
and sorted by game name.

As with MAME, software will be reported as OK only if their directories
contain all the correct tracks with the correct names and nothing else.
Missing files or incorrect files will be reported as BAD,
as will software directories with extra files that need to be removed.

## Splitting a Disc Image

Sometimes a disc image comes as a single `.bin` file
(MAME's `chdman` will extract to this format).
If you would like to turn this file into a set of
Redump-verified tracks, it can be split with

    emuman redump split -r outputdir file.bin

Because combined disk images are identfied by size and hash,
specifying a software list is unnecessary.
