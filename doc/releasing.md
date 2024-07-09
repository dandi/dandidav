`dandidav` Release Process
==========================

These are the steps that the project maintainer should take whenever `dandidav`
reaches a point at which a new release should be cut.  Some steps may be
carried out by any user; only those marked "*(admin)*" must be done by a
repository collaborator or user with write access.

1. Ensure that all dependencies are up to date.  If necessary, create a PR to
   update them.

2. Create a release branch

3. On the release branch, create one or more commits that perform the
   following:

    - Update the version in `Cargo.toml` to the version of the new release.

      <!-- GitHub's Markdown alerts don't work at this indentation level as of
      2024-07-08, so format "IMPORTANT" the normal way -->

      **IMPORTANT:** Even though `dandidav` is not published to crates.io, its
      versioning should still follow [Cargo SemVer][] and the [Rust SemVer
      compatibility guidelines][compat].

    - Run `cargo update -p dandidav` to update the version of `dandidav` listed
      in `Cargo.lock`.

    - Update `CHANGELOG.md` to set the title of the current section to
      `v{version} ({date})`, where `{version}` is the version of the release to
      be made and `{date}` is the current date in YYYY-MM-DD format.

    - If necessary, add the current year to the copyright notice in `LICENSE`.

   It is recommended to do all of this in a single commit and for the commit
   message to be of the form:

    ```text
    v{version} — {short_description}

    {changelog}
    ```

   where `{short_description}` is a brief summary of the release or its
   highlights and `{changelog}` is the body of the changelog section for the
   release (excluding the header).  Note that, if this commit message will be
   used to automatically generate the release notes (see below), then — because
   GitHub treats newlines in release notes as hard line breaks — any paragraphs
   in `{changelog}` that were wrapped across multiple lines (e.g., to enforce
   an 80-column line limit) should be unwrapped into a single line each while
   composing the commit message so that this:

    ```markdown
    - Foo all the bars, and make sure the gnusto is no longer cleesh.
      Fixes [#1](https://github.com/dandi/dandidav/issues/1) via
      [PR #2](https://github.com/dandi/dandidav/pull/2)
      (by [@some-user](https://github.com/some-user))
    ```

    does not end up looking like this:

    > - Foo all the bars, and make sure the gnusto is no longer cleesh.  
    >   Fixes [#1](https://github.com/dandi/dandidav/issues/1) via  
    >   [PR #2](https://github.com/dandi/dandidav/pull/2)  
    >   (by [@some-user](https://github.com/some-user))

    <!--
    Note to people viewing this document's source: For whatever reason, GitHub
    treats newlines as hard line breaks when rendering release notes and issue
    comments (and possibly other things) but not when rendering Markdown
    documents in repositories (like this one).  Thus, in order to force hard
    line breaks when rendering the quote above, each line must end with two
    trailing spaces.

    Do not mess with this.
    -->

4. Push the release branch and create a pull request for it.  The PR should
   *not* have the "skip deployment" label.

5. *(admin)* Merge the release PR.  Performing a rebase-merge is recommended,
   as this results in the `HEAD` commit message afterwards being the same as
   the PR's final commit message.

6. *(admin)* After pulling the latest `main` to a local clone, tag the merged
   `HEAD` with the new version and push the tag.  The recommended commands are:

    ```shell
    git tag -s -m "Version $VERSION" "v$VERSION"
    git push origin "v$VERSION"
    ```

   where `$VERSION` is the version of the new release.  Note that the above
   also signs the tag (recommended); to not sign it, remove the `-s` option
   from `git tag`.

7. *(admin)* Create a GitHub Release for the tag.  The below script (which
   requires [`gh`](https://cli.github.com)) can be used to automatically create
   a release for a given tag by using the tag's commit's commit message's
   subject (i.e., the first line) as the release title and the commit message's
   body (everything after the first line) as the release notes.  If the
   recommended commit message format in step 3 was followed, and the release PR
   was rebase-merged as recommended, this will all work out.

    ```bash
    #!/bin/bash
    set -ex -o pipefail

    tag="${1:?Usage: $0 <tag>}"
    subject="$(git show -s --format=%s "$tag^{commit}")"

    git show -s --format=%b "$tag^{commit}" \
        | gh release create --title "$subject" --notes-file - "$tag"
    ```

8. When the next item is added to `CHANGELOG.md`, place it in a new section
   temporarily titled "In Development".

[Cargo SemVer]: https://doc.rust-lang.org/cargo/reference/resolver.html#semver-compatibility
[compat]: https://doc.rust-lang.org/cargo/reference/semver.html
