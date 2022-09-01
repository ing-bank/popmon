# Release notes

<!--next-version-placeholder-->

## v1.2.0 (2022-09-01)
### Feature
* **report:** Histogram inspector ([`5e78f98`](https://github.com/ing-bank/popmon/commit/5e78f98e4775e12746e62b29f18fee8d7423b8b0))
* Remove time of day from label when midnight or noon ([`1615bed`](https://github.com/ing-bank/popmon/commit/1615bed87450dd603d7ee34ba32dc4379f701e9c))
* **config:** Deprecate skip_empty_plots ([#249](https://github.com/ing-bank/popmon/issues/249)) ([`372ef85`](https://github.com/ing-bank/popmon/commit/372ef85ced7810614f0d828f304284b9157cf534))

### Fix
* Show heatmap descriptions ([`64a4952`](https://github.com/ing-bank/popmon/commit/64a495255b98ab933b791aa31334bf6025139b73))

### Documentation
* **readme:** Include histogram inspector in readme ([`3e68508`](https://github.com/ing-bank/popmon/commit/3e685081f55f4a77413dc238ae1140235689ce49))

## v1.1.0 (2022-08-19)
### Feature
* Extension functionality + diptest implementation @RUrlus ([`8487991`](https://github.com/ing-bank/popmon/commit/8487991380e85635d5580a68b9e09ba33ac9392f))

### Documentation
* **config:** Extend settings documentation ([`fa4d2fc`](https://github.com/ing-bank/popmon/commit/fa4d2fce01018111454d780166c4ca6b042eb146))
* **api:** Restructure api documentation for clarity ([`affdd75`](https://github.com/ing-bank/popmon/commit/affdd75b918ea6201cf2f5879e01fb7b267585c7))
* **profiles:** Profile extension ([`e859530`](https://github.com/ing-bank/popmon/commit/e8595308340ab3d0edba55b7e4d9fcfa66b0beee))
* **comparisons:** Comparison extensions section ([`581e63c`](https://github.com/ing-bank/popmon/commit/581e63c8b926319c9b63bfbe30abd907bd08d3c6))
* Extensions in index ([`6c87fcb`](https://github.com/ing-bank/popmon/commit/6c87fcbd9a0ffe0b5029b5bc2e7fbd6bda3dacd9))
* **readme:** Add section about profile integrations and diptest ([`aa860a7`](https://github.com/ing-bank/popmon/commit/aa860a7193c4a44f378592fe7f8779e3a6af122b))
* **readme:** Include citation information ([#241](https://github.com/ing-bank/popmon/issues/241)) ([`3a29135`](https://github.com/ing-bank/popmon/commit/3a29135fc8ccd7b0a0ba9302eb4281b598ef5608))

## v1.0.0 (2022-07-08)
### Feature
* **report:** Group comparisons by reference key ([#237](https://github.com/ing-bank/popmon/issues/237)) ([`e813534`](https://github.com/ing-bank/popmon/commit/e813534b03ea4662e46f5b782839506fcf49b1ed))
* Entropy profile ([`96dd7d1`](https://github.com/ing-bank/popmon/commit/96dd7d16025db36f9a65149c49c37504cc8f3c7f))
* Configurable title and colors ([`37fcd0e`](https://github.com/ing-bank/popmon/commit/37fcd0e48f4201e1955cd360ce5705a505eeb074))
* Online report CDN for bootstrap, jquery ([`72df86d`](https://github.com/ing-bank/popmon/commit/72df86db4638f7e700857c5fcaf876916e1041d4))
* Plotly express ([`2c2395c`](https://github.com/ing-bank/popmon/commit/2c2395c0b46e7c692bfbb82d4832ab97fbdd0a87))
* Introduce self start reference type ([`ca22268`](https://github.com/ing-bank/popmon/commit/ca22268422100f5fdd932f0bce2fe885c48e62f9))
* String representation for base classes ([`bd480a6`](https://github.com/ing-bank/popmon/commit/bd480a6a134ebdf6c7cba8defca9d77b887b5b76))
* Keep section when changing features ([`7b12d06`](https://github.com/ing-bank/popmon/commit/7b12d061ee329f4a3e8d173a176097fba9d21744))
* **config:** Settings required parameter ([`47d6b17`](https://github.com/ing-bank/popmon/commit/47d6b178ee924d360afabc5b487da1a6ec7d9f8a))
* **registry:** Generalize registry ([`d01c68a`](https://github.com/ing-bank/popmon/commit/d01c68adc9b247ddd382a13b0c7bbd276c9d2ed4))
* **registry:** Add ks, pearson, chi2 to registry ([`4f8126d`](https://github.com/ing-bank/popmon/commit/4f8126df928f948430ef2991af691c30d7199fba))
* **config:** Structured config using pydantic ([`bc52aeb`](https://github.com/ing-bank/popmon/commit/bc52aebc72b1814af3269e99d7a552b383c860cf))

### Fix
* Prevent plot name collision that stops rendering ([#238](https://github.com/ing-bank/popmon/issues/238)) ([`32c7ef4`](https://github.com/ing-bank/popmon/commit/32c7ef47cac005dfbc83dcffd7b28e0df90ccfb6))
* Set time_width in synthetic data example ([`fbf7e41`](https://github.com/ing-bank/popmon/commit/fbf7e41b44d118c539862525f1bf17b57337d05e))
* Guaranteed ordering of traffic light metrics ([`88c3f4a`](https://github.com/ing-bank/popmon/commit/88c3f4a667a80b9f3042797a7996bcbf7cd98690))
* **plot:** Plot_heatmap_b64 `top` argument is now supported ([`8be8115`](https://github.com/ing-bank/popmon/commit/8be811521d23a32f0db7eace353e0ee292c597b4))

### Breaking
* matplotlib-related config is removed  ([`2c2395c`](https://github.com/ing-bank/popmon/commit/2c2395c0b46e7c692bfbb82d4832ab97fbdd0a87))
* Configuration of time_axis, features etc. is moved to the Settings class.  ([`ca22268`](https://github.com/ing-bank/popmon/commit/ca22268422100f5fdd932f0bce2fe885c48e62f9))
* new configuration syntax  ([`bc52aeb`](https://github.com/ing-bank/popmon/commit/bc52aebc72b1814af3269e99d7a552b383c860cf))
* the `plot_metrics` and `plot_overview` settings are no longer available for Tl/alerts  ([`1c4f072`](https://github.com/ing-bank/popmon/commit/1c4f072b74d61f12e239aefbad17edb1787361b3))
* the `worst` entry will no long be present in the datastore  ([`e2b9ef7`](https://github.com/ing-bank/popmon/commit/e2b9ef734d325c88c0aaab58eb4927b72ada9b9c))

### Documentation
* **profiles:** Add entropy ([`8e8ac00`](https://github.com/ing-bank/popmon/commit/8e8ac009d28f3d05b7ca7af79b1995460441e98c))
* Registry snippet to list available profiles/comparisons ([`7760047`](https://github.com/ing-bank/popmon/commit/7760047c4f93aea35622d3b74f84bd588fedc45a))
* **comparisons:** Overview in table ([`cde48c4`](https://github.com/ing-bank/popmon/commit/cde48c446e6eb7f14dc0725b7ec9beb3a04aa29c))
* Profiles in table ([`65459af`](https://github.com/ing-bank/popmon/commit/65459af216231f920e6c09d30daa732e19ea48c3))
* Description for mean trend score ([`b5b7626`](https://github.com/ing-bank/popmon/commit/b5b76264648bb989bd5c0d1b2715ee8812ced458))
* Update api structure ([`c6c53ba`](https://github.com/ing-bank/popmon/commit/c6c53bab953894da3845b70bb5c9ea670d9401e6))
* Documentation for reference types ([`9cf8117`](https://github.com/ing-bank/popmon/commit/9cf8117e9314799af2356a955a484e762100e88e))
* Update api documentation ([`74eb223`](https://github.com/ing-bank/popmon/commit/74eb2231ed6789abe7e383d0930b2d35b6afc29a))
* **config:** Configuration parameter docstrings ([`9eec883`](https://github.com/ing-bank/popmon/commit/9eec883eb65af8b9ed176a8e183e85bca9002fdc))
* **registry:** Instructions on comparison parameter setting ([`2e3b134`](https://github.com/ing-bank/popmon/commit/2e3b1349be6f9a470a3af22ff01dede48a3904c1))
* **config:** Update configuration examples ([`daccc36`](https://github.com/ing-bank/popmon/commit/daccc36eacbb2ac051dd40d2fe252268ac86600b))

### Performance
* Reduce file size of reports ([`329564f`](https://github.com/ing-bank/popmon/commit/329564f729b240bb973bae80dcdbfc7878658c04))

## v0.10.2 (2022-06-21)
### Fix
* Patched histogrammar bin_edges call for Bin histograms ([`590d266`](https://github.com/ing-bank/popmon/commit/590d266f1fc2a5e0d85f26661f6462e31d811103))

## v0.10.1 (2022-06-15)
### Fix
* Patched histogrammar num_bins call for Bin histograms ([`b34fe70`](https://github.com/ing-bank/popmon/commit/b34fe70c854e8dd7a5b062137f68bd358132e86a))

## v0.10.0 (2022-06-14)
### Feature
* **profiles:** Custom profiles via registry pattern ([`d0eb98b`](https://github.com/ing-bank/popmon/commit/d0eb98bd715cf2c03db7f581eb44523bd75092f1))

### Fix
* Protection against outliers in sparse histograms ([#215](https://github.com/ing-bank/popmon/issues/215)) ([`10c3449`](https://github.com/ing-bank/popmon/commit/10c3449a3ab85d1caefc65b52511e11ea49a639e))
* **report:** Traffic light flexbox on small screens ([`2faa7da`](https://github.com/ing-bank/popmon/commit/2faa7da655bf9f446ed599aac718b18e9e2e5fba))

### Documentation
* **synthetic:** Update synthetic examples ([#212](https://github.com/ing-bank/popmon/issues/212)) ([`84a9331`](https://github.com/ing-bank/popmon/commit/84a93314e80e05e54f95e3c02b2ab73d233251d4))
* **readme:** Link profiles and comparisons page ([`17ac6d8`](https://github.com/ing-bank/popmon/commit/17ac6d82a60ca2483eb7e8323dd5a1cfc708833e))
* **profiles:** List the available profiles ([#173](https://github.com/ing-bank/popmon/issues/173)) ([`15f78ec`](https://github.com/ing-bank/popmon/commit/15f78eceaa8f9b29956bfea49276185e21dcd9ca))

## v0.9.0 (2022-05-27)
### Feature
* **report:** Enable the overview section by default ([`22b9cb6`](https://github.com/ing-bank/popmon/commit/22b9cb6c5a7a58a47e1366b39ec9ad3e7722a631))
* **report:** Overview section for quickly navigating reports ([`f5736d4`](https://github.com/ing-bank/popmon/commit/f5736d47c1ce71442e403df1f2bc85d356489e03))
* **report:** Allow section without feature toggle ([`2484569`](https://github.com/ing-bank/popmon/commit/24845693f8ac317bd1b7f3d6f8d9d654f472a066))

### Fix
* **report:** Consistent use of color red ([`453f3fe`](https://github.com/ing-bank/popmon/commit/453f3fe7aed8b96a24f03f2e957b95e5fba51e04))
* **report:** Text contrast and consistent yellow traffic light ([`5d5c43c`](https://github.com/ing-bank/popmon/commit/5d5c43ccf5931d495100f96d3d97a750529e076b))

### Documentation
* **readme:** Replace report image ([`8d363d5`](https://github.com/ing-bank/popmon/commit/8d363d5b975c4143b04ac4074bf8a550ff7857b1))
* **synthetic:** Add dataset overview table ([`8654347`](https://github.com/ing-bank/popmon/commit/86543470396fe55a904d5ce98cede9961b8fc8ea))
* **datasets:** Reference implementations for widely-used publicly available synthetic data streams ([`9988a13`](https://github.com/ing-bank/popmon/commit/9988a13773b1092b76dc317027b4666e35f175f8))

## v0.8.0 (2022-05-20)
### Feature
* **report:** Heatmap time-series for categoricals ([#194](https://github.com/ing-bank/popmon/issues/194)) ([`21c4ad1`](https://github.com/ing-bank/popmon/commit/21c4ad1671b9a4b09d6982a3c496a0aeba459120))
* Nd histogram comparisons and profiles ([`d572f7f`](https://github.com/ing-bank/popmon/commit/d572f7ffa13dd4c22912d908520afb61c5f1db97))
* Dashboarding integration for Kibana ([`83b8869`](https://github.com/ing-bank/popmon/commit/83b88694daee51ff87fc3b6964e1690d1c0c7327))
* Dashboarding integration for Kibana ([`4a9284f`](https://github.com/ing-bank/popmon/commit/4a9284fc25f1c9083020d5cd82ea3b805ec4608d))
* **config:** Global configuration for ing_matplotlib_theme ([`c81e28f`](https://github.com/ing-bank/popmon/commit/c81e28fbd74d3b2497e0a94e4651be3372da58dc))

### Fix
* Import histogrammar specialized ([`d70ab80`](https://github.com/ing-bank/popmon/commit/d70ab80bfc3ee51ecd121dcc054f76941c888796))

### Documentation
* **config:** Global configuration for ing_matplotlib_theme ([`6f4f20d`](https://github.com/ing-bank/popmon/commit/6f4f20d8dc2227958500575bdb1febfd15ae3318))

### Performance
* Directly use bin keys ([`9440897`](https://github.com/ing-bank/popmon/commit/94408977eb414ab3f97b1a9ee17a807ba783b298))
* Disable parallel processing by default ([`85d4407`](https://github.com/ing-bank/popmon/commit/85d4407513d1cb8356835b638241e67bbe68da37))
* Chi2 max residual using numpy ([`8596387`](https://github.com/ing-bank/popmon/commit/85963876394561b7a4c5f03db04f5c59731f4b5d))
* Performant data structure ([`ff72d6e`](https://github.com/ing-bank/popmon/commit/ff72d6e8e8e90d0e90333399dcb2443431f89099))
* Short circuit any/all ([`63c2704`](https://github.com/ing-bank/popmon/commit/63c2704417089dec543ef585ec0e472b31f86c65))
* Optimize pull computation ([`ddf2e35`](https://github.com/ing-bank/popmon/commit/ddf2e359f5d9c417e7cd3cfcbc9c6b24cf0a4282))
* Postpone formatting (expensive for DataFrames) ([`7feaaae`](https://github.com/ing-bank/popmon/commit/7feaaae624f4f0d1f9865009af383085a902d552))
* Compute metrics without report ([`254564c`](https://github.com/ing-bank/popmon/commit/254564c918f09abd1750e8a357c615aa3c5909ac))

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
