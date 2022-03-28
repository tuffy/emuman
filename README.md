# Emulation Manager

This is a command-line based utility for adding and auditing
ROMs for MAME, MAME's software list, MAME's extras,
the Redump database and the No-Intro database.

That is, given a database of what the ROM files are supposed to be,
the ROM files themselves, and where to put them, this will make
sure everything winds up with the correct name and in the right
place so that the games are playable.

It's also *extremely* fast, able to audit MAME's entire game
set in under 10 seconds, and update from one version to the next
in under a minute - even on very modest hardware.

## Installation

At present, the easiest way to install the latest version of `emuman`
is with Rust's cargo:

    cargo install emuman

## Getting Started

The first thing to do is populate the manager's database
of what the ROMs are supposed to be.

### Getting started with MAME

If one has MAME already installed,
one can pipe its XML output directly into our init routine, like:

    mame -listxml | emuman mame init

But if not, the full driver information is available directly from
MAME's download page as `mameXXXXlx.zip` where `XXXX` is the
latest version number.  We can feed our init routine with it like:

    emuman mame init mameXXXXlx.zip

No unzipping required.  Or for added convenience, one can
initialize our database directly from the URL on MAME's website, like:

    emuman mame init https://github.com/mamedev/mame/releases/download/mameXXXX/mameXXXXlx.zip

This will download the Zip file, extract the file and perform the
database initialization.

### Getting started with MAME's software list

Formerly known as MESS, MAME's software list is a database
of the software for all the various home consoles, portables,
personal computers, etc. that MAME supports.  This list is
updated every time MAME is updated.  However, it isn't available
as a single file on MAME's website since it's split into a lot of
individual XML files (at least one per system).

One way to get these files is to populate them from the `hash` directory
of an installed version of MAME, like:

    emuman sl init mameXXXX/hash/*.xml

These files are also provided in MAME's source code dump,
or even straight off the `git` repository of its source code.
Just look for a `hash` directory with lots of XML files.

### Getting started with MAME's extras

In this case, "extras" means things like artwork and snapshots
for MAME, courtesy of Progetto-Snaps.  These are updated
every few versions of MAME.  Look to their
[download page](https://www.progettosnaps.net/snapshots/)
for the latest snapshots downloads.  These are distributed
as a mix of full packs and updates like `pS_XXX_YYYY_ZZZ.zip`
where `XXX` is what sort of extra it is ("title", "logo", etc.),
`YYYY` is whether it's a "fullset" or update ("upd"),
and `ZZZ` is the MAME version.

Our manager can be initialized directly from the zipped files like:

    emuman extra init pS*.zip

This will look in the Zip files and populate our database
with any XML data files it can find.

### Getting started with the No-Intro database

[No-Intro.org](https://no-intro.org/) maintains a set of
XML data files which can be downloaded individually or in bulk.
These are labeled like `<Manufacturer> - <Platform> (<datestamp>-<version>).dat`
and can also be initialized in bulk, like:

    emuman nointro init *.dat

These data files are updated regularly, but one doesn't need
to reinitialize them all every time; specifying only a single
`.dat` file will update only that platform and leave the rest as-is.

### Getting started with the Redump database

[Redump.org](http://redump.org/) also maintains a set of
XML data files which can be downloaded on a per-system basis.
But whereas No-Intro is primarily interested in chip-based media,
Redump focuses on optical discs.  It's XML data files are named like:
`<Manufacturer> - <Platform> - Datfile (<game count>) (<data and timestamp>).dat`
and can also be initialized in bulk, like:

    emuman redump init *.dat

As with No-Intro, the Redump files can also be updated separately.

## Adding the ROM files

At this point, it's important to detail how this ROM manager
differs from every other one out there; instead of packing
several ROM files into one Zip file per game, we leave store
them as one ROM file per file on disk and
(for MAME-related categories) one directory per game.

For instance, instead of packing everything for Mr. Do!
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
and will accept a directory full of machine sub-directories
just as easily as it will accept a directory full of `.zip` files.

But since MAME shares a many of the same ROMs across
many different games, we use hard links to de-duplicate them
into only a single file on disk.  Using `mrdo` and `mrdofix` as
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

Furthermore, using the filesystem's built-in compression
(like ZFS' per-filesystem `compression` setting)
enables us to pack them into less space.  Having the filesystem
be in charge of compression separates concerns and supports
compression types that MAME (or other emulators) may not.

### Adding ROMs for MAME

Now that the manager knows what the ROMs are supposed to be,
we simply need to tell it where to find the ROMs we have
and where to put them.  Like:

    emuman mame add -r MAMEXXXX/roms/ input_dir1/ input_dir2/ ...

Where the `-r` flag indicates our ROM destination and
the `input_dir` paths are where our existing ROMs are now.
The inputs can be individual files, Zip files
(which are scanned for ROMs), directories
(which are scanned recursively) or even URLs to remote files
(which may also be Zip files, and are downloaded and scanned).

Once the target ROM destination has been specified,
the manager will reuse that destination next time
so we don't have to specify it again.

### Adding ROMs for the Software List

This is similar to MAME, but we'll also need to specify
what software list to use for the ROMs.  Like if we're
interested in the Vectrex catalog, try:

    emuman sl add vectrex -r MAMEXXXX/software/vectrex/ input_dir/

As with MAME, this will keep track of the software list
root directory (`MAMEXXXX/software`) so we only need to specify the
target once.

But unlike MAME, the software list has an add-all option
which only requires the software list root directory
and will attempt to add titles to every single software list, like:

    emuman sl add-all -r MAMEXXXX/software/ input_dir/

### Adding extras/ROMs for Snapshots, No-Intro and Redump

These are similar to MAME's Software List in that ROMs
are organized on a per-system basis and the system should be
specified, like:

    emuman nointro add "GCE - Vectrex" -r Vectrex/ input_dir/

The difference is that these don't expect a single "root"
directory like the Software List; the target directories
can be placed wherever.

## Verifying ROM files

Although adding ROMs also performs verification, we may
wish to verify them separately.  If the target directory
has already been defined, this may be as simple as:

    emuman mame verify

The other modes have similar `verify` and `verify-all`
features which verify the ROMs present provide a listing
of files that are bad/missing.

### Upgrading from one version to the next

If the only difference is newly added files,
it's simple to run `add` again to populate the missing files
from an external source.  But when ROM files or entire
games are renamed from one version to the next,
that's when things get complicated.  Therefore,
the easiest way to upgrade is to `add` both the old
files and changed files into a whole new version, like:

    emuman mame add -r MAMEYYYY/roms/ MAMEXXXX/roms/ changed_roms_dir/

Where `XXXX` is the previous version and `YYYY` is the current version.
Then just remove the old `XXXX` version when finished.
Since we hard-link files whenever possible and most ROM files
don't change from one version to the next, this won't take
as much time or space as one might think.

## How we make adding/verification fast

The first time adding or verifying the games for MAME,
the Software List or anything else won't be especially fast;
it may take a half an hour or more to verify hundreds of
thousands of ROM and CHD files, even when leveraging
a good multi-core CPU.

But because most ROMs don't change from one emulator
version to the next, and emulators don't modify ROM files
themselves, we can leverage the filesystem's extended
attributes to drop calculated hash values alongside
the files themselves without modifying their contents
in any way.  And checking an extended attribute is
*a lot* faster than performing a hash of the entire file.
