"""parity-contracts as a repository, pinned to one released version (CR-063).

Like Bazel's `new_local_repository` (the sibling checkout, with a BUILD file this repo owns), and
the fetch fails unless the checkout's VERSION is the version MODULE.bazel pins. IE, the gateway and
parity-store pin the same way (their ci.sh); a contracts release moves every pin line.
"""

def _impl(rctx):
    path = rctx.workspace_root.get_child(rctx.attr.path)
    if not path.is_dir:
        fail("parity-contracts is not at %s" % path)
    have = rctx.read(path.get_child("VERSION"), watch = "yes").strip()
    if have != rctx.attr.version:
        fail(("parity-contracts at %s is %s; MODULE.bazel pins %s. Build against the pinned " +
              "release, or move the pin (MODULE.bazel) with the release.") % (path, have, rctx.attr.version))
    for child in path.readdir():
        rctx.symlink(child, child.basename)
    rctx.delete("BUILD.bazel")
    rctx.symlink(rctx.attr.build_file, "BUILD.bazel")

parity_contracts_repository = repository_rule(
    implementation = _impl,
    attrs = {
        "path": attr.string(mandatory = True, doc = "The checkout, relative to the workspace root"),
        "version": attr.string(mandatory = True, doc = "The pinned contracts release"),
        "build_file": attr.label(mandatory = True),
    },
    local = True,
)
