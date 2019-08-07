# Emulation Manager

This is a ROM manager primarily for MAME that's based
around storing ROM files in non-merged, uncompressed sets.
That means instead of storing all the ROM files for `pacman`
in a `pacman.zip` file, we will create a `pacman` directory
and place all its ROM files in that directory without the use of
zip files at all.

## The Advantages

Storing files this way makes them incredibly easy to audit and manage.
MAME always has significant changes from one release to the next,
and storing files as-is means one can use regular OS file management
utilities to handle them instead of mucking around with compressed
containers.  Need new ROMs?  Just copy them to a machine's directory.
ROMs obsolete?  Just delete them from a machine's directory.
It's as easy as it gets.

### Wouldn't This Waste a Lot of Space?

ROMs don't use a lot of space to begin with compared to CHDs,
and they aren't getting bigger compared to the size of modern hard drives.
Additionally, I use a compressed filesystem to ensure these ROM files
don't waste a lot of space.  Let ZFS be in charge of
keeping them small, while still allowing them to be easy to manage.

### What About BIOS Files?

Although having the same `uni-bios_3_3.rom` in 50 different places
wouldn't take up a lot of space in absolute terms, it's still
not very efficient.  Therefore, this manager will use hard links
so that the same file may be in many places at once, while
still being stored on disk only once.
