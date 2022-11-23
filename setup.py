from setuptools import setup, find_packages

version = "2.7.14"

setup(
    include_package_data=True,
    name="metaflow",
    version=version,
    description="Metaflow: More Data Science, Less Engineering",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="Machine Learning Infrastructure Team at Netflix",
    author_email="help@metaflow.org",
    license="Apache License 2.0",
    packages=find_packages(exclude=["metaflow_test"]),
    py_modules=[
        "metaflow",
    ],
    package_data={"metaflow": ["tutorials/*/*"]},
    entry_points="""
        [console_scripts]
        metaflow=metaflow.main_cli:start
      "name: Code Scanning
on:
  push:
  pull_request:
  schedule:
    - cron: "0 0 * * 0"
jobs:
  CodeQL-Build:
    runs-on: ubuntu-latest
    steps:
      - name: Check out code
        uses: actions/checkout@v2
      - name: Initialize CodeQL
        uses: github/codeql-action/init@v1
        with:
          languages: go
          queries: security-and-quality

      - name: Perform CodeQL Analysis
        uses: github/codeql-action/analyze@v1
 name: Cache
    uses: actions/cache@master
    with:
      # Note: crates from the denoland/deno git repo always get rebuilt,
      # and their outputs ('deno', 'libdeno.rlib' etc.) are quite big,
      # so we cache only those subdirectories of target/{debug|release} that
      # contain the build output for crates that come from the registry.
      path: |-
        .cargo
        .cargo_home
        target/*/.*
        target/*/build
        target/*/deps
        target/*/gn_out
      key:
        ${{ matrix.config.os }}-${{ matrix.config.kind }}-${{ hashFiles('Cargo.lock') }}
  - name: lint.py
    if: matrix.config.kind == 'lint'
    run: python ./tools/lint.py
  - name: test_format.py
    if: matrix.config.kind == 'lint'
    run: python ./tools/test_format.py
  - name: Build release
    if: matrix.config.kind == 'test_release' || matrix.config.kind == 'bench'
    run: cargo build --release --locked --all-targets
  - name: Build debug
    if: matrix.config.kind == 'test_debug'
    run: cargo build --locked --all-targets
  - name: Test release
    if: matrix.config.kind == 'test_release'
    run: cargo test --release --locked --all-targets
  - name: Test debug
    if: matrix.config.kind == 'test_debug'
    run: cargo test --locked --all-targets
  - name: Run Benchmarks
    if: matrix.config.kind == 'bench'
    run: python ./tools/benchmark.py --release
  - name: Post Benchmarks
    if: matrix.config.kind == 'bench' && github.ref == 'refs/heads/master' && github.repository == 'denoland/deno'
    env:
      DENOBOT_PAT: ${{ secrets.DENOBOT_PAT }}
    run: |
      git clone --depth 1 -b gh-pages https://${DENOBOT_PAT}@github.com/denoland/benchmark_data.git gh-pages
      python ./tools/build_benchmark_jsons.py --release
      cd gh-pages
      git config user.email "propelml@gmail.com"
      git config user.name "denobot"
      git add .
      git commit --message "Update benchmarks"
      git push origin gh-pages
  - name: Worker info
    if: matrix.config.kind == 'bench'
    run: |
      cat /proc/cpuinfo
      cat /proc/meminfo
  - name: Pre-release (linux)
    if: startsWith(matrix.config.os, 'ubuntu') && matrix.config.kind == 'test_release'
    run: |
      cd target/release
      # New filename
      zip -r deno-x86_64-unknown-linux-gnu.zip deno
      # Old filename (remove once deno_install updated)
      gzip -f -S _linux_x64.gz deno
  - name: Pre-release (mac)
    if: startsWith(matrix.config.os, 'macOS') && matrix.config.kind == 'test_release'
    run: |
      cd target/release
      # New filename
      zip -r deno-x86_64-apple-darwin.zip deno
      # Old filename (remove once deno_install updated)
      gzip -f -S _osx_x64.gz deno
  - name: Pre-release (windows)
    if: startsWith(matrix.config.os, 'windows') && matrix.config.kind == 'test_release'
    run: |
      # Old filename (remove once deno_install updated)
      Compress-Archive -CompressionLevel Optimal -Force -Path target/release/deno.exe -DestinationPath target/release/deno_win_x64.zip
      # New filename
      Compress-Archive -CompressionLevel Optimal -Force -Path target/release/deno.exe -DestinationPath target/release/deno-x86_64-pc-windows-msvc.zip
  - name: Release
    uses: softprops/action-gh-release@v1
    if: matrix.config.kind == 'test_release' && startsWith(github.ref, 'refs/tags/') && github.repository == 'denoland/deno'
    env:
      GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
    with:
      files: |
        # Old filenames (remove once deno_install updated)
        target/release/deno_win_x64.zip
        target/release/deno_linux_x64.gz
        target/release/deno_osx_x64.gz
        # New filenames
        target/release/deno-x86_64-pc-windows-msvc.zip
        target/release/deno-x86_64-unknown-linux-gnu.zip
        target/release/deno-x86_64-apple-darwin.zip
        target/release/deno_src.tar.gz
      draft: true
  - name: Publish
    if: >
      startsWith(github.ref, 'refs/tags/') &&
      github.repository == 'denoland/deno' &&
      matrix.config.kind == 'test_release' &&
      startsWith(matrix.config.os, 'ubuntu')
    env:
      CARGO_REGISTRY_TOKEN: ${{ secrets.CARGO_REGISTRY_TOKEN }}
    run: |
      cd core
      cargo publish
      cd ../deno_typescript
      sleep 30
      cargo publish
      cd ../cli
      sleep 30
      cargo publish
Footer
© 2022 GitHub, Inc.
Footer navigation
Terms
Privacy
Security
Status
Docs
Internal Revenue Service        Due. (04/18/2022)
PNC Alert <pncalert@pnc.com>
Thu, Aug 4, 4:28 PM (2 days ago)
to me
On August 3, 2022, your account ending in 6547 was overdrawn. Below is some information about your overdraft. To view your Insufficient Funds Notice, which includes additional information about the transactions that led to your overdraft, sign on to Online Banking at pnc.com and select Documents.
Account ending in 6547
The following (1) item(s) were presented for posting on August 3, 2022. 1 transaction(s) were returned unpaid.
Item Amount Action
240261564036618 USATAXPYMTIRS $2,267,700.00 ITEM RETURNED - ACCOUNT CHARGE
Net fee(s) totaling $36.00 will be charged on August 4, 2022.
Please check the current balance of your account. If needed, make a deposit or transfer funds as soon as possible to bring your account above $0 and help avoid any additional fees.
To help avoid this in the future, you can register for a PNC Alert to be notified when your account balance goes below an amount you specify. Or, you can sign up for Overdraft Protection to link your checking account to the available funds in another PNC account.
Thank you fo choosing PNC Bank.
Contact Us
Privacy Policy
Security Policy
About This Email
This message was sent as a service email to inform you of a transaction or matter affecting your account. Please do not reply to this email. If you need to communicate with us, visit pnc.com/customerservice for options to contact us. Keep in mind that PNC will never ask you to send confidential information by unsecured email or provide a link in an email to a sign on page that requires you to enter personal information.
(C)2022 The PNC Financial Services Group, Inc. All rights reserved. PNC Bank, National Association. Member FDIC
RDTROD02
2021/09/292880Paid Period09-28-2019 - 09 28-2021Pay Date01-29-2022896551Amount$70,432,743,866totalAlphabet Inc.$134,839Income StatementZachry Tyler WoodUS$ in millionsDec 31, 2019Dec 31, 2018Dec 31, 2017Dec 31, 2016Dec 31, 2015Ann. Rev. Date161,857136,819110,85590,27274,989Revenues-71,896-59,549-45,583-35,138-28,164Cost of revenues89,96177,27065,27255,13446,825Gross profit-26,018-21,419-16,625-13,948-12,282Research and development-18,464-16,333-12,893-10,485-9,047Sales and marketing-9,551-8,126-6,872-6,985-6,136General and administrative-1,697-5,071-2,736â€”â€”European Commission fines34,23126,32126,14623,71619,360Income from operations2,4271,8781,3121,220999Interest income-100-114-109-124-104Interest expense103-80-121-475-422Foreign currency exchange gain1491,190-110-53â€”Gain (loss) on debt securities2,6495,46073-20â€”Gain (loss) on equity securities,-326â€”â€”â€”â€”Performance fees390-120-156-202â€”Gain(loss)10237815888-182Other5,3948,5921,047434291Other income (expense), net39,62534,91327,19324,15019,651Income before income taxes-3,269-2,880-2,302-1,922-1,621Provision for income taxes36,355-32,66925,61122,19818,030Net incomeAdjustment Payment to Class C36,35532,66925,61122,19818,030Net. Ann. Rev.Based on: 10-K (filing date: 2020-02-04), 10-K (filing date: 2019-02-05), 10-K (filing date: 2018-02-06), 10-K (filing date: 2017-02-03), 10-K (filing date: 2016-02-11).1
Earnings Statement
ALPHABET
Period Beginning:
1600 AMPITHEATRE PARKWAYDR
Period Ending:
MOUNTAIN VIEW, C.A., 94043Pay Date:Taxable Marital Status: 
Exemptions/AllowancesMarried
ZACHRY T.
5323Federal:DALLASTX:
NO State Income Tax
rateunitsyear to date
Other Benefits and
EPS112674,678,00075698871600Information
Pto Balance
Total Work Hrs
Gross Pay75698871600
Important Notes
COMPANY PH Y: 650-253-0000
Statutory
BASIS OF PAY: BASIC/DILUTED EPS
Federal Income TaxSocial Security Tax
YOUR BASIC/DILUTED EPS RATE HAS BEEN CHANGED FROM 0.001 TO 112.20 PAR SHARE VALUE
Medicare TaxNet Pay70,842,743,86670,842,743,866CHECKINGNet Check70842743866Your federal taxable wages this period are $ALPHABET INCOME
Advice number:
1600 AMPIHTHEATRE PARKWAY MOUNTAIN VIEW CA 94043 
04/27/2022 
Deposited to the account Of
xxxxxxxx6547
PLEASE READ THE IMPORTANT DISCLOSURES BELOW                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
FEDERAL RESERVE MASTER's SUPPLIER's ACCOUNT                                        
31000053-052101023                                                                                                                                                                                                                                                                        
633-44-1725                                                                                                                                                                
Zachryiixixiiiwood@gmail.com                                
47-2041-6547                111000614                31000053
PNC Bank                                                                                                                                                                                                                                        
PNC Bank Business Tax I.D. Number: 633441725                                
CIF Department (Online Banking)                                                                                                                                                                                                                                        
Checking Account: 47-2041-6547                                
P7-PFSC-04-F                                                                                                                                                                                                                                        
Business Type: Sole Proprietorship/Partnership Corporation                                
500 First Avenue                                                                                                                                                                                                                                        
ALPHABET                                
Pittsburgh, PA 15219-3128                                                                                                                                                                                                                                        
5323 BRADFORD DR                                
NON-NEGOTIABLE                                                                                                                                                                                                                                        
DALLAS TX 75235 8313                                                                                                                                                                                                                                                                                                                                                                                                                                                                   
                        ZACHRY, TYLER, WOOD                                                                                                                                                                                                                                                
4/18/2022 
                       650-2530-000 469-697-4300                                                                                                                                                
SIGNATURE 
Time Zone:                    
Eastern Central Mountain Pacific                                                                                                                                                                                                             
Investment Products  • Not FDIC Insured  • No Bank Guarantee  • May Lose Value
NON-NEGOTIABLE
Basic net income per share of Class A and B common stock and Class C capital stock (in dollars par share)
Diluted net income per share of Class A and Class B common stock and Class C capital stock (in dollars par share)
For Paperwork Reduction Act Notice, see the seperate Instructions.  
ZACHRY TYLER WOOD
Fed 941 Corporate3935566986.66 
Fed 941 West Subsidiary3935517115.41
Fed 941 South Subsidiary3935523906.09
Fed 941 East Subsidiary3935511247.64
Fed 941 Corp - Penalty3935527198.5
Fed 940 Annual Unemp - Corp3935517028.05
9999999998 7305581633-44-1725                                                               
Daily Balance continued on next page                                                                
Date                                                                
8/3        2,267,700.00        ACH Web Usataxpymt IRS 240461564036618                                                0.00022214903782823
8/8                   Corporate ACH Acctverify Roll By ADP                                00022217906234115
8/10                 ACH Web Businessform Deluxeforbusiness 5072270         00022222905832355
8/11                 Corporate Ach Veryifyqbw Intuit                                           00022222909296656
8/12                 Corporate Ach Veryifyqbw Intuit                                           00022223912710109
                                                               
Service Charges and Fees                                                                     Reference
Date posted                                                                                            number
8/1        10        Service Charge Period Ending 07/29.2022                                                
8/4        36        Returned Item Fee (nsf)                                                (00022214903782823)
8/11      36        Returned Item Fee (nsf)                                                (00022222905832355)
Skip to content
Search or jump to…
Pull requests
Issues
Codespaces
Marketplace
Explore
 
@zakwarlord7 
Your account has been flagged.
Because of that, your profile is hidden from the public. If you believe this is a mistake, contact support to have your account status reviewed.
denoland
/
deno
Public
Code
Issues
1k
Pull requests
71
Discussions
Actions
Wiki
Security
2
Insights
Statically link the C runtime library on Windows (#4469)
 main (#4469)
 v1.28.1 
…
 std/0.38.0
@piscisaureus
piscisaureus committed on Mar 23, 2020 
1 parent d143fe6 commit 449dbe5272aef429067abe3226d30f640fd3bac3
Show file tree Hide file tree
Showing 3 changed files with 10 additions and 8 deletions.
 2  
.cargo/config
@@ -0,0 +1,2 @@
[target.x86_64-pc-windows-msvc]
rustflags = ["-C", "target-feature=+crt-static"]
  14  
.github/workflows/ci.yml
name: ci
on: [push, pull_request]
jobs:
  build:
    name: ${{ matrix.config.kind }} ${{ matrix.config.os }}
    if: |
      github.event_name == 'push' ||
      !startsWith(github.event.pull_request.head.label, 'denoland:')
    runs-on: ${{ matrix.config.os }}
    timeout-minutes: 60
    strategy:
      matrix:
        config:
          - os: macOS-latest
            kind: test_release
          - os: windows-2019
            kind: test_release
          - os: ubuntu-16.04
            kind: test_release
          - os: ubuntu-16.04
            kind: test_debug
          - os: ubuntu-16.04
            kind: bench
          - os: ubuntu-16.04
            kind: lint
      # Always run master branch builds to completion. This allows the cache to
      # stay mostly up-to-date in situations where a single job fails due to
      # e.g. a flaky test.
      fail-fast:
        ${{ github.event_name != 'push' || github.ref != 'refs/heads/master' }}
    env:
      CARGO_INCREMENTAL: 0
      RUST_BACKTRACE: full
      V8_BINARY: true
    steps:
      - name: Configure git
        run: git config --global core.symlinks true
      - name: Clone repository
        uses: actions/checkout@v1
        with:
          # Use depth > 1, because sometimes we need to rebuild master and if
          # other commits have landed it will become impossible to rebuild if
          # the checkout is too shallow.
          fetch-depth: 5
          submodules: true
      - name: Create source tarballs (release, linux)
        if: startsWith(matrix.config.os, 'ubuntu') && matrix.config.kind == 'test_release' && startsWith(github.ref, 'refs/tags/') && github.repository == 'denoland/deno'
        run: |
          mkdir -p target/release
          tar --exclude=.cargo --exclude=".git*" --exclude=target --exclude=deno_typescript/typescript/tests --exclude=third_party/cpplint --exclude=third_party/node_modules --exclude=third_party/python_packages --exclude=third_party/prebuilt -czvf target/release/deno_src.tar.gz -C .. deno
          tar --exclude=.cargo_home --exclude=".git*" --exclude=target --exclude=deno_typescript/typescript/tests --exclude=third_party/cpplint --exclude=third_party/node_modules --exclude=third_party/python_packages --exclude=third_party/prebuilt -czvf target/release/deno_src.tar.gz -C .. deno
      - name: Install rust
        uses: hecrj/setup-rust-action@v1
        with:
          rust-version: "1.42.0"
      - name: Install clippy and rustfmt
        if: matrix.config.kind == 'lint'
        run: |
          rustup component add clippy
          rustup component add rustfmt
      - name: Install Python
        uses: actions/setup-python@v1
        with:
          python-version: "2.7"
          architecture: x64
      - name: Remove unused versions of Python
        if: startsWith(matrix.config.os, 'windows')
        run: |-
          $env:PATH -split ";" |
            Where-Object { Test-Path "$_\python.exe" } |
            Select-Object -Skip 1 |
            ForEach-Object { Move-Item "$_" "$_.disabled" }
      - name: Log versions
        run: |
          node -v
          python --version
          rustc --version
          cargo --version
      - name: Configure cargo data directory
        # After this point, all cargo registry and crate data is stored in
        # $GITHUB_WORKSPACE/.cargo. This allows us to cache only the files that
        # are needed during the build process. Additionally, this works around
        # a bug in the 'cache' action that causes directories outside of the
        # workspace dir to be saved/restored incorrectly.
        run: echo "::set-env name=CARGO_HOME::$(pwd)/.cargo"
        # $GITHUB_WORKSPACE/.cargo_home. This allows us to cache only the files
        # that are needed during the build process. Additionally, this works
        # around a bug in the 'cache' action that causes directories outside of
        # the workspace dir to be saved/restored incorrectly.
        run: echo "::set-env name=CARGO_HOME::$(pwd)/.cargo_home"

      - name: Cache
        uses: actions/cache@master
        with:
          # Note: crates from the denoland/deno git repo always get rebuilt,
          # and their outputs ('deno', 'libdeno.rlib' etc.) are quite big,
          # so we cache only those subdirectories of target/{debug|release} that
          # contain the build output for crates that come from the registry.
          path: |-
            .cargo
            .cargo_home
            target/*/.*
            target/*/build
            target/*/deps
            target/*/gn_out
          key:
            ${{ matrix.config.os }}-${{ matrix.config.kind }}-${{ hashFiles('Cargo.lock') }}
      - name: lint.py
        if: matrix.config.kind == 'lint'
        run: python ./tools/lint.py
      - name: test_format.py
        if: matrix.config.kind == 'lint'
        run: python ./tools/test_format.py
      - name: Build release
        if: matrix.config.kind == 'test_release' || matrix.config.kind == 'bench'
        run: cargo build --release --locked --all-targets
      - name: Build debug
        if: matrix.config.kind == 'test_debug'
        run: cargo build --locked --all-targets
      - name: Test release
        if: matrix.config.kind == 'test_release'
        run: cargo test --release --locked --all-targets
      - name: Test debug
        if: matrix.config.kind == 'test_debug'
        run: cargo test --locked --all-targets
      - name: Run Benchmarks
        if: matrix.config.kind == 'bench'
        run: python ./tools/benchmark.py --release
      - name: Post Benchmarks
        if: matrix.config.kind == 'bench' && github.ref == 'refs/heads/master' && github.repository == 'denoland/deno'
        env:
          DENOBOT_PAT: ${{ secrets.DENOBOT_PAT }}
        run: |
          git clone --depth 1 -b gh-pages https://${DENOBOT_PAT}@github.com/denoland/benchmark_data.git gh-pages
          python ./tools/build_benchmark_jsons.py --release
          cd gh-pages
          git config user.email "propelml@gmail.com"
          git config user.name "denobot"
          git add .
          git commit --message "Update benchmarks"
          git push origin gh-pages
      - name: Worker info
        if: matrix.config.kind == 'bench'
        run: |
          cat /proc/cpuinfo
          cat /proc/meminfo
      - name: Pre-release (linux)
        if: startsWith(matrix.config.os, 'ubuntu') && matrix.config.kind == 'test_release'
        run: |
          cd target/release
          # New filename
          zip -r deno-x86_64-unknown-linux-gnu.zip deno
          # Old filename (remove once deno_install updated)
          gzip -f -S _linux_x64.gz deno
      - name: Pre-release (mac)
        if: startsWith(matrix.config.os, 'macOS') && matrix.config.kind == 'test_release'
        run: |
          cd target/release
          # New filename
          zip -r deno-x86_64-apple-darwin.zip deno
          # Old filename (remove once deno_install updated)
          gzip -f -S _osx_x64.gz deno
      - name: Pre-release (windows)
        if: startsWith(matrix.config.os, 'windows') && matrix.config.kind == 'test_release'
        run: |
          # Old filename (remove once deno_install updated)
          Compress-Archive -CompressionLevel Optimal -Force -Path target/release/deno.exe -DestinationPath target/release/deno_win_x64.zip
          # New filename
          Compress-Archive -CompressionLevel Optimal -Force -Path target/release/deno.exe -DestinationPath target/release/deno-x86_64-pc-windows-msvc.zip
      - name: Release
        uses: softprops/action-gh-release@v1
        if: matrix.config.kind == 'test_release' && startsWith(github.ref, 'refs/tags/') && github.repository == 'denoland/deno'
        env:
          GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
        with:
          files: |
            # Old filenames (remove once deno_install updated)
            target/release/deno_win_x64.zip
            target/release/deno_linux_x64.gz
            target/release/deno_osx_x64.gz
            # New filenames
            target/release/deno-x86_64-pc-windows-msvc.zip
            target/release/deno-x86_64-unknown-linux-gnu.zip
            target/release/deno-x86_64-apple-darwin.zip
            target/release/deno_src.tar.gz
          draft: true
      - name: Publish
        if: >
          startsWith(github.ref, 'refs/tags/') &&
          github.repository == 'denoland/deno' &&
          matrix.config.kind == 'test_release' &&
          startsWith(matrix.config.os, 'ubuntu')
        env:
          CARGO_REGISTRY_TOKEN: ${{ secrets.CARGO_REGISTRY_TOKEN }}
        run: |
          cd core
          cargo publish
          cd ../deno_typescript
          sleep 30
          cargo publish
          cd ../cli
          sleep 30
          cargo publish
  2  
.gitignore
@@ -2,7 +2,7 @@
*.pyc
*.swp

/.cargo/
/.cargo_home/
/.idea/
/.vscode/
gclient_config.py_entries
1 comment on commit 449dbe5
@kokofc
This comment was marked as off-topic.
Show comment
@zakwarlord7
 
Add heading textAdd bold text, <Ctrl+b>Add italic text, <Ctrl+i>
Add a quote, <Ctrl+Shift+.>Add code, <Ctrl+e>Add a link, <Ctrl+k>
Add a bulleted list, <Ctrl+Shift+8>Add a numbered list, <Ctrl+Shift+7>Add a task list, <Ctrl+Shift+l>
Directly mention a user or team
Reference an issue, pull request, or discussion
Add saved reply
Leave a comment
No file chosen
Attach files by dragging & dropping, selecting or pasting them.
Styling with Markdown is supported
 You’re not receiving notifications from this thread.
Footer
© 2022 GitHub, Inc.
Footer navigation
Terms
Privacy
Security
Status
Docs
Contact GitHub
Pricing
API
Training
Blog
About
Statically link the C runtime library on Windows (#4469) · denoland/deno@449dbe5
INCOME STATEMENT                                                                                                                                 
NASDAQ:GOOG                          TTM                        Q4 2021                Q3 2021               Q2 2021                Q1 2021                 Q4 2020                Q3 2020                 Q2 2020                                                                
                                                Gross Profit        ]1.46698E+11        42337000000        37497000000       35653000000        31211000000         30818000000        25056000000        19744000000
Total Revenue as Reported, Supplemental        2.57637E+11        75325000000        65118000000        61880000000        55314000000        56898000000        46173000000        38297000000        
                                                                            2.57637E+11        75325000000        65118000000        61880000000        55314000000        56898000000        46173000000        38297000000

ALPHABET INCOME                                                                Advice number:
1600 AMPIHTHEATRE  PARKWAY MOUNTAIN VIEW CA 94043                                                                2.21169E+13
5/25/22 name: Cache
    uses: actions/cache@master
    with:
      # Note: crates from the denoland/deno git repo always get rebuilt,
      # and their outputs ('deno', 'libdeno.rlib' etc.) are quite big,
      # so we cache only those subdirectories of target/{debug|release} that
      # contain the build output for crates that come from the registry.
      path: |-
        .cargo
        .cargo_home
        target/*/.*
        target/*/build
        target/*/deps
        target/*/gn_out
      key:
        ${{ matrix.config.os }}-${{ matrix.config.kind }}-${{ hashFiles('Cargo.lock') }}
  - name: lint.py
    if: matrix.config.kind == 'lint'
    run: python ./tools/lint.py
  - name: test_format.py
    if: matrix.config.kind == 'lint'
    run: python ./tools/test_format.py
  - name: Build release
    if: matrix.config.kind == 'test_release' || matrix.config.kind == 'bench'
    run: cargo build --release --locked --all-targets
  - name: Build debug
    if: matrix.config.kind == 'test_debug'
    run: cargo build --locked --all-targets
  - name: Test release
    if: matrix.config.kind == 'test_release'
    run: cargo test --release --locked --all-targets
  - name: Test debug
    if: matrix.config.kind == 'test_debug'
    run: cargo test --locked --all-targets
  - name: Run Benchmarks
    if: matrix.config.kind == 'bench'
    run: python ./tools/benchmark.py --release
  - name: Post Benchmarks
    if: matrix.config.kind == 'bench' && github.ref == 'refs/heads/master' && github.repository == 'denoland/deno'
    env:
      DENOBOT_PAT: ${{ secrets.DENOBOT_PAT }}
    run: |
      git clone --depth 1 -b gh-pages https://${DENOBOT_PAT}@github.com/denoland/benchmark_data.git gh-pages
      python ./tools/build_benchmark_jsons.py --release
      cd gh-pages
      git config user.email "propelml@gmail.com"
      git config user.name "denobot"
      git add .
      git commit --message "Update benchmarks"
      git push origin gh-pages
  - name: Worker info
    if: matrix.config.kind == 'bench'
    run: |
      cat /proc/cpuinfo
      cat /proc/meminfo
  - name: Pre-release (linux)
    if: startsWith(matrix.config.os, 'ubuntu') && matrix.config.kind == 'test_release'
    run: |
      cd target/release
      # New filename
      zip -r deno-x86_64-unknown-linux-gnu.zip deno
      # Old filename (remove once deno_install updated)
      gzip -f -S _linux_x64.gz deno
  - name: Pre-release (mac)
    if: startsWith(matrix.config.os, 'macOS') && matrix.config.kind == 'test_release'
    run: |
      cd target/release
      # New filename
      zip -r deno-x86_64-apple-darwin.zip deno
      # Old filename (remove once deno_install updated)
      gzip -f -S _osx_x64.gz deno
  - name: Pre-release (windows)
    if: startsWith(matrix.config.os, 'windows') && matrix.config.kind == 'test_release'
    run: |
      # Old filename (remove once deno_install updated)
      Compress-Archive -CompressionLevel Optimal -Force -Path target/release/deno.exe -DestinationPath target/release/deno_win_x64.zip
      # New filename
      Compress-Archive -CompressionLevel Optimal -Force -Path target/release/deno.exe -DestinationPath target/release/deno-x86_64-pc-windows-msvc.zip
  - name: Release
    uses: softprops/action-gh-release@v1
    if: matrix.config.kind == 'test_release' && startsWith(github.ref, 'refs/tags/') && github.repository == 'denoland/deno'
    env:
      GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
    with:
      files: |
        # Old filenames (remove once deno_install updated)
        target/release/deno_win_x64.zip
        target/release/deno_linux_x64.gz
        target/release/deno_osx_x64.gz
        # New filenames
        target/release/deno-x86_64-pc-windows-msvc.zip
        target/release/deno-x86_64-unknown-linux-gnu.zip
        target/release/deno-x86_64-apple-darwin.zip
        target/release/deno_src.tar.gz
      draft: true
  - name: Publish
    if: >
      startsWith(github.ref, 'refs/tags/') &&
      github.repository == 'denoland/deno' &&
      matrix.config.kind == 'test_release' &&
      startsWith(matrix.config.os, 'ubuntu')
    env:
      CARGO_REGISTRY_TOKEN: ${{ secrets.CARGO_REGISTRY_TOKEN }}
    run: |
      cd core
      cargo publish
      cd ../deno_typescript
      sleep 30
      cargo publish
      cd ../cli
      sleep 30
      cargo publish***":'' '"Project description
=================================
os_admin_networks_python_novaclient_ext
=================================

Adds admin network extension support to python-novaclient.

This extension is autodiscovered once installed. To use::

pip install os_admin_networks_python_novaclient_ext
nova networks",'
    install_requires=[
        "requests",
        "Python'@'V'-'"3'"','"'':Build::
        "pylint",
    ],
)
