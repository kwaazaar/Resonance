Resonance - A super lightweight eventing library, with support for heavyweight scenarios!
-----------------------------------------------------------------------------------------
Resonance requires a database for storage. Both MS SQL server and MySql are supported. To create
the database (or add the required tables to an existing database), you must execute the
SQL database script first (there are different scripts for both DBMSs).

To get the database script:
- It may have been added to the root of your product when installing the Resonance.Core package.
- When you project is a NETCore project, this probably didn't work, so you need to download
  it manually:
  - MS Sql Server: https://raw.githubusercontent.com/kwaazaar/Resonance/master/Resonance.Core/content/ResonanceDB.MsSql.sql
  - MySql: https://raw.githubusercontent.com/kwaazaar/Resonance/master/Resonance.Core/content/ResonanceDB.MySql.sql

A sample console app (.NET Core) can be found here: https://github.com/kwaazaar/Resonance-Cli-Demo

Good luck!