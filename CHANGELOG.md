# Release notes

<!--next-version-placeholder-->

## v0.7.0 (2022-05-09)
### Feature
* Global configuration of joblib Parallel backend ([`3431cad`](https://github.com/ing-bank/popmon/commit/3431cad4dc2ec3e00755d08c7870f6464a65393e))

### Fix
* Prevent numpy warnings ([`9ec3b66`](https://github.com/ing-bank/popmon/commit/9ec3b66b3679435cc4a55ac8a7afb6b45e295964))

### Documentation
* **config:** Document global configuration ([`e546994`](https://github.com/ing-bank/popmon/commit/e546994eb11d020adbb4a056b7d01978926b2f4f))
* **readme:** Extend articles section ([`0ec0273`](https://github.com/ing-bank/popmon/commit/0ec0273cb920a3585b25acfc98c3d6cbcfbf7456))

## v0.6.1 (2022-04-29)
### Fix
* **plot:** Fixing memory leak in matplotlib multithreading ([`cc6c4e1`](https://github.com/ing-bank/popmon/commit/cc6c4e16a38267c3d5e80d12b986c458854ba781))

### Documentation
* Include link to kedro-popmon ([`aff68b7`](https://github.com/ing-bank/popmon/commit/aff68b72fb7cdf2872ac84d020044086263b0f76))

## v0.6.0 (2021-12-13)
### Feature
* **comparisons:** Introduce psi and jsd ([`c6a1ca7`](https://github.com/ing-bank/popmon/commit/c6a1ca74da3a4de1f69b919cce047cc0b6377589))
* **comparisons:** Introduce comparison registry ([`031c146`](https://github.com/ing-bank/popmon/commit/031c146be6ac04b6e2c1dabc17a28a7aa2b778f9))

### Documentation
* **comparisons:** Add comparisons page ([`60967c9`](https://github.com/ing-bank/popmon/commit/60967c901f16b8c3acba6d8c8976fccf859f458b))
* Fix broken link ([`2b38fc6`](https://github.com/ing-bank/popmon/commit/2b38fc6c8464a9d24dbb5bdf3108f51d8b101a3a))
* **rtd:** Install popmon ([`e9c4610`](https://github.com/ing-bank/popmon/commit/e9c46106c8004b219e6ca5ad0eed4fce7f464a6c))

## v0.5.0 (2021-11-24)
### Feature
* Improve pipeline visualization ([`bb09d73`](https://github.com/ing-bank/popmon/commit/bb09d730d275e4a97d0d7174d8a325e8c98bea44))

### Fix
* Ensure uniqueness of apply_funcs_key ([`ba98c97`](https://github.com/ing-bank/popmon/commit/ba98c973c8e27fe69ce1c3a82c4fa14abba3d818))

### Documentation
* **rtd:** Migrate config to v2 ([`a8d9f76`](https://github.com/ing-bank/popmon/commit/a8d9f76b5f999615e623dc1a5b37287a42bad841))
* Refresh notebooks ([#151](https://github.com/ing-bank/popmon/issues/151)) ([`0bccc7e`](https://github.com/ing-bank/popmon/commit/0bccc7e4e7725fee00d37b1279fb8988dacccbec))
* Pipeline visualizations in docs and notebooks ([`913bfb0`](https://github.com/ing-bank/popmon/commit/913bfb0aec607ea68567ecc71e65cfae7c86ff75))
* Changelog md syntax ([`b187d36`](https://github.com/ing-bank/popmon/commit/b187d360bc303b347826c77eb356d2d4dcc5ad38))
* Specify requirements ([`e3f6b0a`](https://github.com/ing-bank/popmon/commit/e3f6b0aa431d886c92d3324080dc7460950dabb7))

## [v0.4.4](https://github.com/ing-bank/popmon/compare/v0.4.3...v0.4.4) (2021-10-22)


### 📖 Documentation

* notebook on report interpretation ([dd3be73](https://github.com/ing-bank/popmon/commits/dd3be73b8bf8b602722104d61663ccbd2f5ac64e))


### 🐛 Bug fixes

* distinct incorrect result (constant) ([3c47cde](https://github.com/ing-bank/popmon/commits/3c47cdef37b3abeb96d75dcc343f5c3ea8d87695))
* prevent division by zero ([bacf8dd](https://github.com/ing-bank/popmon/commits/bacf8dd4581150abffb603aab201ebf85971de6a))
* uu-chi2 incorrect return ([e259d3e](https://github.com/ing-bank/popmon/commits/e259d3e7601881ca5ee8e4b36ee489b3b9d1fe32))


## [v0.4.3](https://github.com/ing-bank/popmon/compare/v0.4.2...v0.4.3) (2021-10-04)


### 🐛 Bug fixes

* fix too restrictive numpy integer check in hist_stitcher ([c162f11](https://github.com/ing-bank/popmon/commits/c162f11a68a6d8aaf82cb9fd8365f018cbc2feb6))

## [v0.4.2](https://github.com/ing-bank/popmon/compare/v0.4.1...v0.4.2) (2021-08-25)

### ⬆️ Dependencies

* Pin ing-matplotlib-theme dependency to >=0.1.8 (closes #131) ([1bca302b20f2434a8ea0dea195e974d3b2ed3da3](https://github.com/ing-bank/popmon/commit/1bca302b20f2434a8ea0dea195e974d3b2ed3da3))

## [v0.4.1](https://github.com/ing-bank/popmon/compare/v0.4.0...v0.4.1) (2021-06-23)


### 🎉 Features

* tabular traffic light/alerts overviews ([1d31265](https://github.com/ing-bank/popmon/commits/1d312653e0f2e788f4631839d201460f7e4ff562))


### 👷‍♂️ Internal improvements

* add utils to prevent code repetition ([d4a28cb](https://github.com/ing-bank/popmon/commits/d4a28cbfe6e3dcc8cf5ed1d92b1d679eb06aaab7))
* convert dict constructor to dict literal ([331d8b1](https://github.com/ing-bank/popmon/commits/331d8b17620d90fef24360232fcdf2ab84e40b92)),  ([b3d4d88](https://github.com/ing-bank/popmon/commits/b3d4d889cc4a38a4f2a0d29d87c5ec4ba417cfbf)), ([9e5188c](https://github.com/ing-bank/popmon/commits/9e5188c80dd709885311a2e041a06c86c6caa897)), ([c9518ab](https://github.com/ing-bank/popmon/commits/c9518abc1e52789193d928e336eddb38d2b5881e))
* directly pass arguments ([bf33807](https://github.com/ing-bank/popmon/commits/bf338075f4dee3c8e00e8997497e32063482b8f3))


### 🐛 Bug fixes

* copyright in license ([b16fd99](https://github.com/ing-bank/popmon/commits/b16fd993a0c4bed2ee50991ac2863e2f196c65ae)), ([60a22dd](https://github.com/ing-bank/popmon/commits/60a22dd36fa0ec9c2a6a6dc97eccb72509841f16))
* extend TL config ([eba10ed](https://github.com/ing-bank/popmon/commits/eba10edb5d784c6a7644cef999d42bad22a8d7a2))
* mismatch between stats displayed in the traffic lights section and comparisons due to pattern matching bug. ([5ffb0a9](https://github.com/ing-bank/popmon/commits/5ffb0a97e032f5500f65d01a88a93f2b60e99471))


### 📖 Documentation

* add explanation for hiding green TLs by default ([81095af](https://github.com/ing-bank/popmon/commits/81095af73b5b0157c4d6d373142a49747deda22b))
* codemotion article ([1e20f30](https://github.com/ing-bank/popmon/commits/1e20f304567afd1edac987521c8026e2146f5d17))
* include changelog in docs ([a2237d0](https://github.com/ing-bank/popmon/commits/a2237d088439ecd563fc68ad82696855bf8f6ec4))


### ⬆️ Dependencies

* pre-commit updates ([6f55155](https://github.com/ing-bank/popmon/commits/6f55155a920de83e33c6129ddd85ecaf682eff47)), ([8fc5518](https://github.com/ing-bank/popmon/commits/8fc5518c5963f6a9dfdcf5e7f8025faa69b0f983)), ([e02adf1](https://github.com/ing-bank/popmon/commits/e02adf1dab9e30815fec4150eedbb01e15a81509))
* **histogrammar:** popmon working with hgr v1.0.23 ([#101](https://github.com/ing-bank/popmon/issues/101)) ([d4a986e](https://github.com/ing-bank/popmon/commits/d4a986ed0add983721d2a60fbefc385fe2ed7ed3))
* Pin ing-matplotlib-theme dependency to master (closes #131) ([1e20f30](https://github.com/ing-bank/popmon/commit/f1ed364045d4286ab3e144034fd8cf2c1e3aef89))


## v0.4.0 and before

The release notes for preceding versions are available [here](https://github.com/ing-bank/popmon/blob/master/CHANGES.rst>).
