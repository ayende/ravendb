"use strict";

var gulp = require("gulp"),
    concat = require("gulp-concat"),
    cssnano = require("gulp-cssnano"),
    concatCss = require('gulp-concat-css'),
    htmlmin = require("gulp-htmlmin"),
    uglify = require("gulp-uglify"),
    merge = require("merge-stream"),
    processhtml = require('gulp-processhtml'),
    del = require("del"),
    bundleconfig = require("./bundleconfig.json");


var PATHS = {
    outputDir: "../artifacts"
}

gulp.task("build-Debug", [/* Do nothing */]);
gulp.task("build-Release", ["release"]);
gulp.task("build-Profiling", [/* Do nothing */]);

gulp.task('release', ['min', 'release-process-index', 'release-copy-favicon', 'release-copy-optimized-build', 'release-copy-images', 'release-copy-fonts', 'release-copy-ext-libs']);

gulp.task("min", ["min:ext-js", "min:app-js", "min:css"]);

gulp.task('release-copy-ext-libs', function () {
    return gulp.src(["Scripts/ace/**/*.*",
        "Scripts/forge/**/*.*",
        "Scripts/moment.js",
        "Scripts/d3/**/*.*",
        "Scripts/text.js",
        "Scripts/require.js",
        "Scripts/jszip/**/*.*"], { base: 'Scripts/' })
        .pipe(uglify())
        .pipe(gulp.dest(PATHS.outputDir + "/Html5/Scripts/"))
});

gulp.task('release-copy-favicon', function () {
    return gulp.src("favicon.ico")
        .pipe(gulp.dest(PATHS.outputDir + "/Html5/"));
});

gulp.task('release-copy-optimized-build', function () {
    return gulp.src(['optimized-build/**/*.*', '!optimized-build/App/main.js'])
       .pipe(gulp.dest(PATHS.outputDir + "/Html5/"));
});

gulp.task('release-copy-images', function () {
    return gulp.src('Content/images/*')
       .pipe(gulp.dest(PATHS.outputDir + "/Html5/Content/images/"));
});

gulp.task('release-copy-fonts', function () {
    return gulp.src('fonts/*')
       .pipe(gulp.dest(PATHS.outputDir + "/Html5/fonts"));
});

gulp.task('release-process-index', function () {
    return gulp.src('index.html')
        .pipe(processhtml())
        .pipe(gulp.dest(PATHS.outputDir + "/Html5"));
});

gulp.task("min:app-js", function () {
    return gulp.src(["optimized-build/App/main.js"])
        .pipe(concat(PATHS.outputDir + "/Html5/App/main.js"))
        .pipe(uglify())
        .pipe(gulp.dest("."));
});

gulp.task("min:ext-js", function () {
    var tasks = getBundles(".js").map(function (bundle) {
        return gulp.src(bundle.inputFiles, { base: "." })
            .pipe(concat(bundle.outputFileName))
            .pipe(uglify())
            .pipe(gulp.dest("."));
    });
    return merge(tasks);
});

gulp.task("min:css", function () {
    var tasks = getBundles(".css").map(function (bundle) {
        return gulp.src(bundle.inputFiles, { base: "." })
            .pipe(concatCss(bundle.outputFileName, { rebaseUrls: false }))
            .pipe(cssnano())
            .pipe(gulp.dest("."));
    });
    return merge(tasks);
});

gulp.task("clean", function () {
    del.sync([PATHS.outputDir + "/Html5/", PATHS.outputDir + "/Raven.Studio.Html5.zip"], { force: true });
});

function getBundles(extension) {
    return bundleconfig.filter(function (bundle) {
        return new RegExp(extension).test(bundle.outputFileName);
    });
}